Cover the RTSP watchdog's decision loop — the failure threshold, the reset on recovery, and the cold-boot initial delay — by parameterizing it over its probe and driving it on tokio's virtual clock.
