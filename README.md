Mojang Snooper Data Parser

This just pulls some of the values out of the JSON data and fires it into MySQL, nothing too fancy.
Data is fed into the parser via stdin, I use zcat to push the files into it, check out the wrapper script for an example.
