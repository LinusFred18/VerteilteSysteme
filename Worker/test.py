import hashlib

payload = "2 3 4"
value = 0

for number in payload.split():
    value = value + int(number)

print(value)

print(len(payload))



value = 0
for number in payload.split():
    value = value + int(number)
value = value/len(payload.replace(" ",""))
print("Average", value)


hash = hashlib.sha256((payload).encode('utf-8'))
print(hash)

payload2 = "1 7"

payload2 = payload2.replace(" ","")
number = int(payload2)
if number < 2:
    print("A")
for i in range(2, int(number ** 0.5) + 1):
    if number % i == 0:
        print("A")
print("B")


print(payload[::-1])