Log a warning when a NATS command payload doesn't decode, instead of dropping the command in silence with the `playout_commands` counter still recording it as received.
