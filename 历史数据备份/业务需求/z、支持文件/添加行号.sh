cat new_*.txt | awk '{print NR "#_#" $0}'| sed "s/\\x23\\x5F\\x23/\\x80/g" | split -a 4 --additional-suffix=.dat -d -l $CNT --numeric-suffixes=1 - a_10000_"$1"_"$2"_00_
rm -f new_*.txt


for i in $(ls)  
  do  
      cat $i | awk '{print NR "||" $0}'  > ../$i
done