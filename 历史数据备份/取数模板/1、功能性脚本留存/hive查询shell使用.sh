#! /bin/bash
num=$(beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -f /home/udbac/output/test/test.hql)

echo $num