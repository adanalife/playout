The RTSP DESCRIBE reader is now tested against a status line split across packets and against a relay that hangs up mid-response — the read loop had no test that made it iterate.
