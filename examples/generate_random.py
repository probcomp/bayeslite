import random
import sys

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print "Usage: python generate_random.py <# rows> <# dimensions>"
        sys.exit()

    N = int(sys.argv[1])
    D = int(sys.argv[2])

    names = ['x' + str(i) for i in range(D)]
    data = [[random.random() < 0.5 for j in range(D)] for i in range(N)]

    print ','.join(names)
    for row in data:
        print ','.join(['1' if r else '0' for r in row])
 
