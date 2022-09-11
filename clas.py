# To create the class withn the stdent
class person():
    def __init__(self,x,y):
         self.name=x
         self.add=y
    def talk (self):
        print('hi I am',self.name)
        print('my address is',self.add)

p1=person('20','30')
p1.talk()