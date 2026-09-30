"""Read local clocks and bounded NTP estimates without adjusting system time.

Notes
-----
- This standalone standard-library diagnostic runs on Windows or via container
  stdin. It never imports trading adapters, submits orders, or sets a clock.
- NTP estimates are unauthenticated diagnostics, not trading calibration.
"""

import argparse
import json
import platform
import socket
import struct
import time


_NTP_EPOCH = 2_208_988_800


def local_sample():
    """Bracket the wall clock with monotonic reads and retain platform clocks."""
    before = time.monotonic_ns()
    wall = time.time_ns()
    after = time.monotonic_ns()
    sample = {"wall_ns": wall, "mono_ns": (before + after) // 2,
              "read_span_ns": after - before}
    for name in ("CLOCK_MONOTONIC_RAW", "CLOCK_BOOTTIME"):
        if hasattr(time, name):
            sample[name] = time.clock_gettime_ns(getattr(time, name))
    return sample


def _unix_ns(data):
    """Decode an NTP era-zero timestamp, valid until February 2036."""
    seconds, fraction = struct.unpack("!II", data)
    return (seconds - _NTP_EPOCH) * 1_000_000_000 + (fraction * 1_000_000_000 >> 32)


def probe(server, timeout):
    """Return an NTP estimate or explicit failure; never correct a local clock.

    Parameters
    ----------
    server
        NTP hostname queried over UDP port 123.
    timeout
        Maximum socket wait in seconds; the CLI permits up to five seconds.

    Notes
    -----
    - Positive offset means the remote clock is ahead of this process.
    - The network-only interval permits asymmetric transit. Server clock error
      and root dispersion are not included in that interval.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
            connection.settimeout(timeout)
            connection.connect((server, 123))
            packet = bytearray(48)
            packet[0] = 0x23
            sent = local_sample()
            seconds, fraction = divmod(sent["wall_ns"], 1_000_000_000)
            packet[40:48] = struct.pack("!II", seconds + _NTP_EPOCH, (fraction << 32) // 1_000_000_000)
            connection.send(packet)
            response = connection.recv(512)
            received = local_sample()
        if (len(response) < 48 or response[0] >> 6 == 3 or response[0] & 7 != 4
                or not 1 <= response[1] <= 15 or response[24:32] != packet[40:48]):
            raise ValueError("Invalid, unsynchronized, or unmatched NTP response")
        t1, t4 = sent["wall_ns"], received["wall_ns"]
        t2, t3 = _unix_ns(response[32:40]), _unix_ns(response[40:48])
        wall_elapsed = t4 - t1
        mono_elapsed = received["mono_ns"] - sent["mono_ns"]
        step = wall_elapsed - mono_elapsed
        valid = abs(step) <= 20_000_000 and t3 >= t2 and wall_elapsed >= t3 - t2
        return {"server": server, "status": "estimate" if valid else "clock_discontinuity",
                "stratum": response[1], "observed_wall_ns": t4,
                "round_trip_ms": mono_elapsed / 1e6,
                "wall_minus_mono_elapsed_ms": step / 1e6,
                "offset_ms": ((t2 - t1) + (t3 - t4)) / 2e6 if valid else None,
                "network_only_offset_interval_ms": [(t3 - t4) / 1e6, (t2 - t1) / 1e6] if valid else None,
                "root_dispersion_ms": struct.unpack("!I", response[8:12])[0] / 65536 * 1000}
    except (OSError, ValueError) as error:
        return {"server": server, "status": "unavailable", "reason": str(error)}


def main():
    """Print bounded JSON diagnostics suitable for a host/container comparison."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="time.google.com")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=2)
    args = parser.parse_args()
    if not 1 <= args.samples <= 10 or not 0 < args.timeout <= 5:
        parser.error("Use 1-10 samples and a timeout in (0, 5] seconds")
    print(json.dumps({"platform": platform.platform(), "kind": "start", "clocks":local_sample()}), flush=True)
    for _ in range(args.samples):
        print(json.dumps({"kind": "ntp_probe", **probe(args.server, args.timeout), "clocks": local_sample()}), flush=True)
        time.sleep(1)
    print(json.dumps({"kind": "end", "clocks":local_sample()}), flush=True)


if __name__ == "__main__":
    main()
