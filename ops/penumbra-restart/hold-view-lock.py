import fcntl, sys, time
f = open(sys.argv[1], "a+")
fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
print("view-db lock held", flush=True)
while True: time.sleep(3600)
