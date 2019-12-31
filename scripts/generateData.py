from random import *

# total number of reducers
TR = 8
# total number of mappers
TM = 8
# maximum size of one task
#MaxSize = 10000
MaxSize = 1000
# frontier between majority and minority : MaxSize//TM
#MinSize = 625
MinSize = 62
# max number of tasks per reducer
#NbT = 10000
NbT = 10000

nb_tasks_per_reducer = {}
total_number_of_tasks = 0

class Data(object):
  # one Data object represents 1 line in a Data file
  def __init__(self,one_key, one_reducer, one_mapper):
    self.key = one_key # task key
    self.reducer = one_reducer # reducer number
    self.val1 = randrange(1,99999) # artificial data
    self.val2 = randrange(1,99999)
    self.val3 = randrange(1,99999)
    self.mapper = one_mapper # mapper number
    
  def to_string(self):
    # CSV line
    return str(self.key)+";"+str(self.reducer)+";"+str(self.val1)+";"+str(self.val2)+";"+str(self.val3)+";"+str(self.mapper)

def generateFileForMapper(i,f):
  # generate file f, for the mapper i
  # So i between 1 and TM
  cpt = 0
  t = 1
  taskSize = randrange(MinSize,MaxSize)+1
  nl=1 # number of lines (i.e. number of values) of current task

  # generate data for B1(ri)
  # B1 is the first bundle, tasks of which I am the biggest owner
  while (t <= (nb_tasks_per_reducer[i]//4)):
    f.write(Data(t*1000+i,i,i).to_string()+"\n")
    cpt = cpt+1
    nl = nl+1
    if (nl > taskSize):
      # next task
      nl=1
      t=t+1
      taskSize = randrange(MinSize,MaxSize+1)+1
  base = t

  # generate data for B2(ri)
  # B2 is the second bundle, tasks of which I am a simple owner, not the biggest 
  nl=1
  taskSize = randrange(MinSize)+1
  while (t-base <= (nb_tasks_per_reducer[i]//2)):
    f.write(Data(t*1000+i,i,i).to_string()+"\n")
    cpt = cpt+1
    nl = nl+1
    if (nl > taskSize):
      # next task
      nl=1
      t=t+1
      taskSize = randrange(MinSize)+1

  # generate data for B3(rj), j!=i
  # B3 is the third bundle, distant tasks 
  for j in range(TR):
    j = j+1 ; # now, j is between 1 and TR
    if (j != i):
      # generate non local data for reducer j
      tj=1 # task number
      nl=0 # number of lines of current task
      taskSize = randrange(MaxSize//TM)
      while (tj <= (nb_tasks_per_reducer[j])):
        if (nl <= taskSize):
          # generate lines for this task
          f.write(Data(tj*1000+j,j,i).to_string()+"\n")
          cpt = cpt+1
          nl = nl+1
        else:
          # next task
          nl=1
          tj=tj+1
          taskSize = randrange(MaxSize//TM)


# s is the seed
def generate(s):
  seed(s);
  global nb_tasks_per_reducer 
  global total_number_of_tasks
  # drawing lots for a number of tasks per reducer, for each reducer
  for i in range(TR):
    nb_tasks = randrange(NbT)
    total_number_of_tasks = total_number_of_tasks + nb_tasks
    nb_tasks_per_reducer[i+1]=nb_tasks
    print("reducer "+str(i+1)+" -> "+ str(nb_tasks) + " taches ")
  
  for i in range(TM):
    # one file per mapper, an existing file with the same name will be erased
    #f_out = open("/mnt/Data/Artificial/mapper"+str(i+1)+".txt","w")
    f_out = open("./mapper"+str(i+1)+".txt","w")
    print("fichier du mapper "+str(i+1))
    generateFileForMapper(i+1, f_out)
    f_out.close()

generate(10)
