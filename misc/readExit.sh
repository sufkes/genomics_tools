#!/usr/bin/env bash
if [ -z "$1" ]
then
    out_dir="$(pwd)"
else
    out_dir="$1"
fi
echo "out_dir: ""$out_dir"
cat /home/sufkes/bin/c4r_step_numbers.txt |
    while read line
    do
	echo "$line"
	name="$(echo "$line" | cut -d " " -f 2)"
	find "$out_dir"/job_output/"$name" -name *.o -type f 2> /dev/null | while read ff;
	do
	    echo -e "\t""$(basename "$ff")"
	    echo -e "\t\t""$(tail -n 1 "$ff" | grep MUGQICexitStatus)"
	done
	echo 
    done

