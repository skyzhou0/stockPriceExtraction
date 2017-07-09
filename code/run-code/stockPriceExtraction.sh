cd /Users/haozhou/Desktop/vm-window-tunel/workspace/forecastEventData

STR="stockprice"
fileFormat=".csv"
dateToday=$(date +"%d%m%y")
fileName=$STR$dateToday$fileFormat
#echo $fileName
curl -o $fileName -s "http://download.finance.yahoo.com/d/quotes.csv?s=MONY.L&f=nd1t1l1k3hgvpa2"
#curl "http://download.finance.yahoo.com/d/quotes.csv?s=GOCO.L&f=nd1t1l1k3hgvpa2" >> $fileName 
#curl "http://download.finance.yahoo.com/d/quotes.csv?s=^FTSE&f=nd1t1l1k3hgvpa2" >> $fileName 
#curl "http://download.finance.yahoo.com/d/quotes.csv?s=^FTMC&f=nd1t1l1k3hgvpa2" >> $fileName 
#curl "http://download.finance.yahoo.com/d/quotes.csv?s=ADM.L&f=nd1t1l1k3hgvpa2" >> $fileName 
#curl "http://download.finance.yahoo.com/d/quotes.csv?s=ZMA.SG&f=nd1t1l1k3hgvpa2" >> $fileName 
#echo UPDATED:
#date
#echo "extraction completed"
