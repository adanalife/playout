Thumbnail frame grabs keep their concurrency slot until the decode finishes, and a whole grab ends within its 10-second budget, so a burst of stills can no longer run more than two decodes at once.
