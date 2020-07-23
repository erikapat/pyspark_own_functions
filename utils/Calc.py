# Class that group several related functions.
class Calc:
    def __init__(self,df): #self as a variable whose 
                            #sole job is to learn the name 
                            #of a particular instance, below this name is x    
        self.df = df
    @property
    def shape(self):
        return (self.df.count(), len(self.df.columns))
