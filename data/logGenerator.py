import random
import time

infos = [
'116.397026,39.918058',
'116.410886,39.881949',
'116.272876,39.99243',
'116.544079,40.417555',
'116.225404,40.258186',
'116.38631,39.937209',
'116.399466,39.989743'
]

phones = [
"13526667788","15622223333","15677778888",
"13526667788","15622223333","15677778888",
"13526667788","15622223333","15677778888",
"13526667788","15622223333","15677778888",
]

def info_sample():
    return random.sample(infos,1)[0]

def phone_sample():
    return random.sample(phones,1)[0]

def generate_log(num = 5):
    str_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open("/Users/harold/Documents/Code/storm-train/data/storm.log","a+") as f:
        while num > 0:
            str = "{phone}\t{info}\t[{local_time}]".format(phone=phone_sample(), info=info_sample(), local_time=str_time);
            f.write(str + "\n")
            num -= 1



if __name__ == '__main__':
    generate_log()