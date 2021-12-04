#install-tools:
#open https://github.com/xitongsys/parquet-go/tree/master/tool/parquet-tools
#open https://www.infoq.cn/article/in-depth-analysis-of-parquet-column-storage-format

file=alltypes_plain.parquet

parquet-tools -cmd schema -file $file -tag false

echo '----'

parquet-tools -cmd cat -count 2 -file $file | jq

