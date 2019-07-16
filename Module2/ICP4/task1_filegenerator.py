from random import randint
import time
a=1
with open('hobbit.txt') as file:
    lines=file.readlines()
    while a<=30:
        totalline=len(lines)
        linenumber=randint(0,totalline-1)
        with open('/home/apandit7/BigDataProgramming/ICP4/log/log{}.txt'.format(a),'w') as writefile:
            writefile.write(' '.join(line for line in lines[linenumber: totalline]))
        print('creating file log{}.txt'.format(a))
        a+=1
        time.sleep(5)
