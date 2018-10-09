import pandas as pd



old_file = pd.read_excel('/home/nicolaske/Downloads/inbev_co recalc.xlsx')


new_name = pd.DataFrame(old_file,columns=['probe_fk'])

for i in xrange(len(new_name)):
   print str(new_name.loc[i]['probe_fk'])+',',