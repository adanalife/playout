Still-frame grabs no longer ratchet playout's memory up until the pod is OOMKilled: glibc malloc arenas are capped at two and each still decodes on a single thread.
