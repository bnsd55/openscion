The server refuses a data directory that group or others can read, as the
daemon refuses a socket directory: readable is reachable, and whoever can
read the directory has every blob and every row. A directory that only
group or others can write was already refused; the refusal now demands
mode 0700 and the error says so (`chmod 700 <dir>`), and a restore makes
its target 0700 whatever the umask. An operator whose data directory was
mode 0755 must run `chmod 700 <dir>` before the server starts again.
