#!/usr/bin/env python3
"""Regression test for nanomq/nanomq#2355 (shared-subscription part).

QoS-1 messages published while a shared-subscription ($share/<group>/<filter>)
persistent session (clean_start=false) is offline used to be dropped at the
transport offline-cache branch: the topic was matched against the raw stored
filter including the $share/<group>/ prefix, so it never matched and the
message was freed instead of stored. A resumed session got nothing, ever.

Two broker configurations are exercised per storage backend:

1. Default (resend_on_ack absent): storage + eventual redelivery only.
   Redelivery pace is the existing resend timer (short retry interval), so
   completeness is asserted within a generous window and a minimum-elapsed
   floor proves the drain stayed off. A plain subscription runs as a
   control: after the fix, shared members must behave identically to plain
   subscribers on resume. An overlap case ($share QoS-0 filter subscribed
   before a plain QoS-1 filter for the same topic) guards the
   strongest-match QoS resolution in the offline-cache branch.

2. resend_on_ack=true with a large retry interval (60s): the ack-clocked
   drain must deliver the whole backlog within seconds of reconnect,
   exactly once - the first timer fire would be at 90s, so every message
   inside the assertion window proves the drain, not the timer. Includes
   live publishes interleaved into a stalled drain (manual acks) and
   SQLite persistence across a broker restart (deliberately timer-paced:
   msg timestamps are not comparable across processes).

Runs its own broker instances on a dedicated port; safe to call from
test.py while the shared broker owns 1883.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

HOST = "127.0.0.1"
PORT = 1899
QOS_DURATION = 2           # short retry interval: redelivery is timer-paced
RECV_WINDOW = 30.0         # generous window for the timer to drain the backlog
MSG_COUNT = 3
# with 3 msgs at 1 per timer fire (first at 1.5 x qos_duration = 3s), a
# complete timer-paced drain cannot finish before ~7s; anything faster
# would mean the drain armed although resend_on_ack is off
TIMER_FLOOR = 4.0
DRAIN_QOS_DURATION = 60    # first timer fire at 90s: outside every window
DRAIN_WINDOW = 15.0        # ack-clocked drain must finish well inside this


def find_nanomq():
    if os.environ.get("NANOMQ_BIN"):
        return os.environ["NANOMQ_BIN"]
    found = shutil.which("nanomq")
    if found:
        return found
    repo_root = Path(__file__).resolve().parents[2]
    local = repo_root / "build" / "nanomq" / "nanomq"
    if local.exists():
        return str(local)
    raise FileNotFoundError("nanomq binary not found (PATH, NANOMQ_BIN, build/nanomq)")


def write_conf(workdir, sqlite_enabled, resend_on_ack=False):
    conf = f'listeners.tcp {{ bind = "0.0.0.0:{PORT}" }}\n'
    if resend_on_ack:
        conf += "mqtt { resend_on_ack = true }\n"
    if sqlite_enabled:
        # redelivery pace is --qos_duration for both backends; the broker-
        # level sqlite block has no resend_interval key (bridge-only)
        conf += (
            "sqlite {\n"
            "    disk_cache_size = 102400\n"
            f'    mounted_file_path = "{workdir}/"\n'
            "    flush_mem_threshold = 1\n"
            "}\n"
        )
    path = os.path.join(workdir, "nanomq_2355.conf")
    with open(path, "w") as f:
        f.write(conf)
    return path


def wait_for_port(timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def wait_port_free(timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.2):
                time.sleep(0.2)
        except OSError:
            return True
    return False


def start_broker(conf_path, workdir, qos_duration=QOS_DURATION):
    log_path = os.path.join(workdir, "nanomq_2355.log")
    cmd = [
        find_nanomq(), "start",
        "--conf", conf_path,
        "--qos_duration", str(qos_duration),
        "--log_level", "warn",
        "--log_stdout", "false",
        "--log_file", log_path,
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_for_port():
        stop_broker(proc)  # reap and free the port before raising
        raise RuntimeError("broker did not open port %d" % PORT)
    return proc


def stop_broker(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    wait_port_free()


def make_client(client_id):
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
    )
    return client


def session_properties():
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = 3600
    return props


def subscribe_then_disconnect(client_id, subs):
    # subs: list of (filter, qos); subscribed sequentially so subinfol
    # keeps the given order
    client = make_client(client_id)
    subscribed = []

    def on_subscribe(cl, userdata, mid, reason_code_list, properties):
        # a SUBACK reason code >= 128 is a rejected subscription; fail
        # here with a clear message instead of a confusing 0/3 later
        subscribed.append(all(not rc.is_failure for rc in reason_code_list))

    client.on_subscribe = on_subscribe
    client.connect(HOST, PORT, keepalive=60, clean_start=False,
                   properties=session_properties())
    client.loop_start()
    for sub_filter, qos in subs:
        client.subscribe(sub_filter, qos=qos)
    deadline = time.time() + 5
    while len(subscribed) < len(subs) and time.time() < deadline:
        time.sleep(0.05)
    client.disconnect()
    client.loop_stop()
    if len(subscribed) < len(subs):
        print("issue_2355: SUBACK not received for %s" % (subs,))
        return False
    if not all(subscribed):
        print("issue_2355: subscription rejected for %s" % (subs,))
        return False
    return True


def publish_messages(topic, qos, count=MSG_COUNT, prefix="msg-"):
    pub = make_client("issue2355-pub")
    pub.connect(HOST, PORT, keepalive=60, clean_start=True)
    pub.loop_start()
    for i in range(count):
        info = pub.publish(topic, payload="%s%d" % (prefix, i), qos=qos)
        info.wait_for_publish(timeout=5)
        # wait_for_publish returns silently on timeout; fail loudly here
        # instead of as a confusing 0/N assertion later
        if qos > 0 and not info.is_published():
            pub.loop_stop()
            raise RuntimeError("publish of %s%d never acked" % (prefix, i))
    pub.disconnect()
    pub.loop_stop()


def reconnect_and_collect(client_id, window=RECV_WINDOW, expected=MSG_COUNT,
                          resubscribe=None):
    received = {}
    connected = []

    def on_message(cl, userdata, m):
        payload = m.payload.decode()
        received[payload] = received.get(payload, 0) + 1

    def on_connect(cl, userdata, flags, rc, props=None):
        connected.append(flags.session_present)
        # MQTT spec: session_present=false means the server holds no
        # session state (subscriptions are not persisted across a broker
        # restart) and the client must subscribe again
        if resubscribe and not flags.session_present:
            cl.subscribe(resubscribe, qos=1)

    client = make_client(client_id)
    client.on_message = on_message
    client.on_connect = on_connect
    start = time.time()
    client.connect(HOST, PORT, keepalive=60, clean_start=False,
                   properties=session_properties())
    client.loop_start()
    deadline = time.time() + window
    while time.time() < deadline and len(received) < expected:
        time.sleep(0.1)
    elapsed = time.time() - start
    # small grace period to catch unexpected extra deliveries
    time.sleep(1.0)
    client.disconnect()
    client.loop_stop()
    session_present = connected[0] if connected else None
    return received, elapsed, session_present


def run_case(name, client_id, subs, pub_topic, window=RECV_WINDOW,
             min_elapsed=None, drain=False):
    print("issue_2355: case '%s' start" % name)
    if not subscribe_then_disconnect(client_id, subs):
        return False
    time.sleep(0.5)
    publish_messages(pub_topic, qos=1)
    received, elapsed, session_present = reconnect_and_collect(
        client_id, window=window)

    expected_payloads = ["msg-%d" % i for i in range(MSG_COUNT)]
    ok = True
    if sorted(received.keys()) != sorted(expected_payloads):
        print("issue_2355: case '%s' FAILED: got %d/%d distinct msgs: %s"
              % (name, len(received), MSG_COUNT, sorted(received.keys())))
        ok = False
    dups = {k: v for k, v in received.items() if v > 1}
    if ok and dups:
        if drain:
            # the drain keeps exactly one message in flight and the
            # timer cannot fire inside the window: a duplicate means a
            # message was fetched twice
            print("issue_2355: case '%s' FAILED: duplicate deliveries: %s"
                  % (name, dups))
            ok = False
        else:
            # timer-paced redelivery may legitimately re-send with DUP
            # if an ack races the next timer fire; informational only
            print("issue_2355: case '%s' note: duplicate deliveries: %s"
                  % (name, dups))
    if ok and drain and elapsed > window:
        print("issue_2355: case '%s' FAILED: backlog took %.1fs "
              "(timer-paced although resend_on_ack=true?)" % (name, elapsed))
        ok = False
    if ok and min_elapsed is not None and elapsed < min_elapsed:
        print("issue_2355: case '%s' FAILED: backlog completed in %.1fs "
              "(< %.1fs floor: drain armed although resend_on_ack is off?)"
              % (name, elapsed, min_elapsed))
        ok = False
    if ok and session_present is not True:
        print("issue_2355: case '%s' FAILED: session_present=%s"
              % (name, session_present))
        ok = False
    if ok:
        print("issue_2355: case '%s' ok (%d msgs in %.1fs, session_present=%s)"
              % (name, len(received), elapsed, session_present))
    return ok


def run_live_during_drain(client_id, sub_filter, pub_topic, live_count=2):
    """Publish live traffic while the backlog drain is stalled mid-flight.

    Uses manual acking to hold the first drained message unacked (the
    ack-clocked drain cannot proceed), interleaves live publishes, then
    releases the acks. Every payload - backlog and live - must arrive
    exactly once: live in-flight rows must not be re-sent by the drain.
    """
    print("issue_2355: case 'live-during-drain' start")
    if not subscribe_then_disconnect(client_id, [(sub_filter, 1)]):
        return False
    time.sleep(0.5)
    publish_messages(pub_topic, qos=1)

    received = {}
    to_ack = []
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        manual_ack=True,
    )

    def on_message(cl, userdata, m):
        payload = m.payload.decode()
        received[payload] = received.get(payload, 0) + 1
        to_ack.append(m)

    client.on_message = on_message
    client.connect(HOST, PORT, keepalive=60, clean_start=False,
                   properties=session_properties())
    client.loop_start()

    ok = True
    # wait for the first drained backlog msg; it stays unacked, so the
    # drain is stalled while we interleave live publishes
    deadline = time.time() + 5
    while not to_ack and time.time() < deadline:
        time.sleep(0.05)
    if not to_ack:
        print("issue_2355: case 'live-during-drain' FAILED: no backlog "
              "msg arrived while holding acks")
        ok = False
    else:
        publish_messages(pub_topic, qos=1, count=live_count,
                         prefix="live-")
        time.sleep(1.0)  # live msgs are delivered independently
        # release acks (including for msgs that keep arriving) until the
        # whole backlog + live set is in or the window closes
        expected = MSG_COUNT + live_count
        deadline = time.time() + DRAIN_WINDOW
        while time.time() < deadline:
            while to_ack:
                m = to_ack.pop(0)
                client.ack(m.mid, m.qos)
            if len(received) >= expected and not to_ack:
                break
            time.sleep(0.1)
        time.sleep(1.0)
        while to_ack:
            m = to_ack.pop(0)
            client.ack(m.mid, m.qos)

    client.disconnect()
    client.loop_stop()

    if ok:
        expected_payloads = sorted(
            ["msg-%d" % i for i in range(MSG_COUNT)]
            + ["live-%d" % i for i in range(live_count)])
        if sorted(received.keys()) != expected_payloads:
            print("issue_2355: case 'live-during-drain' FAILED: got %s "
                  "expected %s" % (sorted(received.keys()), expected_payloads))
            ok = False
        dups = {k: v for k, v in received.items() if v > 1}
        if ok and dups:
            print("issue_2355: case 'live-during-drain' FAILED: duplicate "
                  "deliveries: %s" % dups)
            ok = False
    if ok:
        print("issue_2355: case 'live-during-drain' ok (%d msgs, all "
              "exactly once)" % len(received))
    return ok


def run_qos0_control(client_id, sub_filter, pub_topic):
    # QoS-0 messages must not be stored for offline sessions, shared or not
    print("issue_2355: case 'qos0-control' start")
    if not subscribe_then_disconnect(client_id, [(sub_filter, 1)]):
        return False
    time.sleep(0.5)
    publish_messages(pub_topic, qos=0)
    # a regression that stored QoS-0 would redeliver on the resend timer:
    # first fire is qos_duration * 1.5 = 3s after resume, so listen across
    # two timer periods to actually catch it
    received, _, _ = reconnect_and_collect(client_id, window=7.0, expected=1)
    if received:
        print("issue_2355: case 'qos0-control' FAILED: QoS0 offline msgs "
              "were delivered: %s" % sorted(received.keys()))
        return False
    print("issue_2355: case 'qos0-control' ok (0 msgs, as expected)")
    return True


def dump_broker_log(workdir):
    # test.py's print_nanomq_log() shows the main 1883 broker; on failure
    # the log that matters is this test's own instance on port 1899
    log_path = os.path.join(workdir, "nanomq_2355.log")
    try:
        with open(log_path) as f:
            print("issue_2355: broker log %s:" % log_path)
            print(f.read())
    except OSError as exc:
        print("issue_2355: cannot read broker log %s: %s" % (log_path, exc))


def run_backend(sqlite_enabled):
    label = "sqlite" if sqlite_enabled else "memory"
    workdir = tempfile.mkdtemp(prefix="nanomq2355-%s-" % label)
    conf = write_conf(workdir, sqlite_enabled)
    broker = None
    ok = False
    try:
        broker = start_broker(conf, workdir)
        plain_ok = run_case("%s/plain-control" % label,
                            "issue2355-%s-plain" % label,
                            [("t2355/plain/x", 1)], "t2355/plain/x",
                            min_elapsed=TIMER_FLOOR)
        shared_ok = run_case("%s/shared" % label,
                             "issue2355-%s-shared" % label,
                             [("$share/g1/t2355/share/x", 1)],
                             "t2355/share/x", min_elapsed=TIMER_FLOOR)
        # an overlapping shared QoS-0 filter subscribed first must not
        # shadow the plain QoS-1 filter in the offline-cache match: the
        # strongest matching subscription decides the stored QoS
        overlap_ok = run_case("%s/overlap" % label,
                              "issue2355-%s-ov" % label,
                              [("$share/g1/t2355/ov/x", 0),
                               ("t2355/ov/x", 1)], "t2355/ov/x")
        qos0_ok = run_qos0_control("issue2355-%s-qos0" % label,
                                   "$share/g1/t2355/qos0/x", "t2355/qos0/x")
        # assigned only after all cases ran: an exception mid-run leaves
        # ok False so the finally block still dumps the broker log
        ok = plain_ok and shared_ok and overlap_ok and qos0_ok
    finally:
        if broker is not None:
            stop_broker(broker)
        if not ok:
            dump_broker_log(workdir)
        shutil.rmtree(workdir, ignore_errors=True)
    return ok


def run_drain_backend(sqlite_enabled):
    # resend_on_ack=true with a 60s retry interval: the first timer fire
    # would be at 90s, so every message inside DRAIN_WINDOW proves the
    # ack-clocked drain
    label = "sqlite" if sqlite_enabled else "memory"
    workdir = tempfile.mkdtemp(prefix="nanomq2355-drain-%s-" % label)
    conf = write_conf(workdir, sqlite_enabled, resend_on_ack=True)
    broker = None
    ok = False
    try:
        broker = start_broker(conf, workdir,
                              qos_duration=DRAIN_QOS_DURATION)
        plain_ok = run_case("drain-%s/plain" % label,
                            "issue2355-dr-%s-plain" % label,
                            [("t2355/plain/x", 1)], "t2355/plain/x",
                            window=DRAIN_WINDOW, drain=True)
        shared_ok = run_case("drain-%s/shared" % label,
                             "issue2355-dr-%s-shared" % label,
                             [("$share/g1/t2355/share/x", 1)],
                             "t2355/share/x",
                             window=DRAIN_WINDOW, drain=True)
        live_ok = True
        restart_ok = True
        if sqlite_enabled:
            live_ok = run_live_during_drain("issue2355-dr-livedrain",
                                            "t2355/live/x", "t2355/live/x")
            # backlog must survive a broker restart (persisted in SQLite).
            # Across a restart the drain does not apply (msg timestamps
            # are per-process clocks); redelivery runs on the resend
            # timer, so the restarted broker uses a short retry interval
            # and the assertion is on completeness, not burst timing.
            print("issue_2355: case 'drain-sqlite/restart' start")
            cid = "issue2355-dr-restart"
            if not subscribe_then_disconnect(cid, [("t2355/restart/x", 1)]):
                restart_ok = False
            else:
                time.sleep(0.5)
                publish_messages("t2355/restart/x", qos=1)
                time.sleep(1.0)  # let rows land before terminating
                stop_broker(broker)
                broker = start_broker(conf, workdir, qos_duration=2)
                # subscriptions are in-memory only: the restarted broker
                # reports session_present=false and the client must
                # re-subscribe for the persisted backlog to be delivered
                received, elapsed, _ = reconnect_and_collect(
                    cid, window=30.0, resubscribe="t2355/restart/x")
                if len(received) != MSG_COUNT:
                    print("issue_2355: case 'drain-sqlite/restart' FAILED: "
                          "%d/%d msgs after restart: %s"
                          % (len(received), MSG_COUNT,
                             sorted(received.keys())))
                    restart_ok = False
                elif elapsed < TIMER_FLOOR:
                    # the conf still has resend_on_ack=true: enforce the
                    # documented guarantee that sessions restored across
                    # a restart are always timer-paced (msg timestamps
                    # are not comparable between processes)
                    print("issue_2355: case 'drain-sqlite/restart' FAILED: "
                          "completed in %.1fs (< %.1fs floor: drain armed "
                          "across a restart?)" % (elapsed, TIMER_FLOOR))
                    restart_ok = False
                else:
                    print("issue_2355: case 'drain-sqlite/restart' ok "
                          "(%d msgs in %.1fs, timer-paced)"
                          % (len(received), elapsed))
        # assigned only after all cases ran: an exception mid-run leaves
        # ok False so the finally block still dumps the broker log
        ok = plain_ok and shared_ok and live_ok and restart_ok
    finally:
        if broker is not None:
            stop_broker(broker)
        if not ok:
            dump_broker_log(workdir)
        shutil.rmtree(workdir, ignore_errors=True)
    return ok


def issue_2355_test():
    ok = True
    for sqlite_enabled in (True, False):
        # return False instead of raising: test.py's harness expects the
        # return-False convention and has no try/except around test calls
        try:
            if not run_backend(sqlite_enabled):
                ok = False
        except Exception as exc:
            print("issue_2355: backend run crashed: %s" % exc)
            ok = False
        try:
            if not run_drain_backend(sqlite_enabled):
                ok = False
        except Exception as exc:
            print("issue_2355: drain backend run crashed: %s" % exc)
            ok = False
    if ok:
        print("issue_2355: all cases passed")
    else:
        print("issue_2355: FAILED")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if issue_2355_test() else 1)
