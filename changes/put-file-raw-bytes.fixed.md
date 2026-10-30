`scn commit --put PATH=FILE` takes a local FILE whose name is not UTF-8,
as `checkout --into` and `mount` take a local directory: the name's
bytes name the file. Before, clap refused the whole argument ("invalid
UTF-8 was detected") before the command ran. PATH, the repo path, is
still UTF-8, and a refusal now says so.
