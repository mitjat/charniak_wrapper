Wrapper around Charniak's parser, allowing to use it one sentence at a time
while the engine is running. It parallelizes parsing by using a pool of parser 
processes in the background. More importantly, it gracefully handles inputs 
for which the Charniak parser crashes or hangs.

Usage:
  1. As a library: call init() first, then parse_sentence(txt) for each sentence.
     At teardown, you might want kill_parsers()
  2. As a standalone web service: just run it (port and hostname hardcoded below);
     for every line of input that you send to the socket, you receive a line of output,
     i.e. the parsed sentence.
