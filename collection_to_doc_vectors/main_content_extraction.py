from boilerpipe.extract import Extractor
import sys


def __input():
    ret = ''
    for line in sys.stdin:
        ret += line + '\n' \

    return ret


if __name__ == '__main__':
    print(Extractor(html=__input()).getText())
