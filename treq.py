# 3.class vars and class methods
class Sample:
 x = 10
 @classmethod
 def modify(cls):
  cls.x+=1
# create two instances
s1 = Sample()
s2 = Sample()
print(s1.x, s2.x)
# modify s1 in the class namespace
s1.modify()
print(s1.x, s2.x)
# modify s1 in the instance namespace
s1.x+=1
print(s1.x, s2.x)
