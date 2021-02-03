import threading

def increment_global():
    global x
    x += 1

def thread_task():
    for _ in range(50000):
        increment_global()

def safe_thread_task(lock):
    for _ in range(50000):
        lock.acquire()
        increment_global()
        lock.release()

def main():
    # global variable
    global x
    x = 0
    lock = threading.Lock()
    t1 = threading.Thread(target=thread_task)
    t2 = threading.Thread(target=thread_task)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

def safe_main():
    # global variable
    global x
    x = 0
    lock = threading.Lock()
    t1 = threading.Thread(target=safe_thread_task, args=(lock,))
    t2 = threading.Thread(target=safe_thread_task, args=(lock,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

if __name__ == “__main__”:
    print("Unsafe")
    for i in range(5):
        main()
        print("x = {1} after Iteration {0}".format(i,x))
    #  output is random (race condition due to side effect)
    # x = 71470 after Iteration 0
    # x = 82789 after Iteration 1
    # x = 79652 after Iteration 2
    # x = 83459 after Iteration 3
    # x = 100000 after Iteration 4

    print("Safe")
    for i in range(5):
        safe_main()
        print("x = {1} after Iteration {0}".format(i,x))
    # No problem
    # x = 100000 after Iteration 0
    # x = 100000 after Iteration 1
    # x = 100000 after Iteration 2
    # x = 100000 after Iteration 3
    # x = 100000 after Iteration 4
