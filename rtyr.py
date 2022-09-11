# 10.overring the base class constructor and method in sub class
# cut the contructor in Son class and re-execute this program
class Father:
    def __init__(self):
        self.property = 800000.00
    def display_property(self):
        print('Father\'s property= ', self.property)
class Son(Father):
        def __init__(self):
            self.property = 200000.00
        def display_property(self):
            print('Child\'s property= ', self.property)
# create sub class instance and display father's property
s = Father()
s.display_property()