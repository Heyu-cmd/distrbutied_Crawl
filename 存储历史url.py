from bitarray import bitarray
import mmh3
import hashlib

offset = 214748467 // (2**31 -1)
bit_array = bitarray(4*1024*1024*1024)
bit_array.setall(0)

url = "http://www.baidu.com"
# mmh3 hash计算出来的是有符号整型，加上offset使结果在0-2**31 -1区间里
# url_hash = mmh3.hash(url, 42)
b1 = hashlib.md5(url.encode(encoding='UTF-8')).hexdigest()
bit_array[b1] = 1