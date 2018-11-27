#!/bin/bash

#返回值 echo $
#利用 ad=`sts $1 $2` 获取返回值

function sts(){
strd=$1
strh=$2
if [ ${#strh} == 1 ]
then
std=${strd}0${strh}
elif [ ${#strh} == 2 ]
then
std=${strd}${strh}
else
std=${strd}00
fi
echo ${std}
}

function main(){
winDay=(20181119)
hive -e "truncate table tmp.mid_expoure;"
for wday in ${winDay[@]}
do
  for((i=0;i<12;i+=1))
  do
  winDate=`sts ${wday} ${i}`
  echo "winDate==>"${winDate}
    num=1
    addTime=0
    while(( ${num} <= 48 ))
    do
    at=$[ ${i}+${addTime} ]
    inte=$(( $at / 24))
    re=$(( $at % 24 ))
    #echo "inte=>"$inte "re=>"$re
    ad=$[ ${wday} + ${inte} ]
    ah=${re}
    #echo $ad   $ah
    reqDate=`sts ${ad} ${ah}`
    echo "reqDate<=>"${reqDate}
    #hql
    hive -e "
        set hive.exec.parallel=true;
        insert into table tmp.mid_expoure
        select
            '${winDate}' as data_date,
            '${reqDate}' as relate_date,
            ori.win_ori as win_ori,
            er.expoure_relate as expoure_relate,
            er.expoure_relate/ori.win_ori as expoure_per,
            cr.click_relate as win_relate,
            cr.click_relate/ori.win_ori as win_per
        from
            (select
                count(distinct(req_id)) as win_ori
            from ssp.ods_f_win win
            where win.data_date = '${winDate}') ori,
            (select
                count(*) as expoure_relate
            from ssp.ods_f_win win ,ssp.ods_f_exposure exposure
            where win.req_id = exposure.req_id
                and win.data_date = '${winDate}'
                and exposure.data_date >= '${winDate}'
                and exposure.data_date <= '${reqDate}') er,
            (select
                count(*) as click_relate
            from ssp.ods_f_win win ,ssp.ods_f_click click
            where win.req_id = click.req_id
                and win.data_date = '${winDate}'
                and click.data_date >= '${winDate}'
                and click.data_date <= '${reqDate}') cr;
        "
    ((num=num+1))
    ((addTime=addTime+1))

    done
  data=""
  done
done
}

main
#a=2
#b=24
#c=$(( $a % $b ))
#d=$(( $a / $b ))
#echo $c $d#!/usr/bin/env bash