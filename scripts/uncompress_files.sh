if [ ! $# -eq 1 ]
  then
      echo "./$0 <crawl_id>"
      exit
fi

crawl_id="$1"

temp_folder=$(python -m crawler.configuration crawler project_folder)"/tmp/"
mkdir -p "$temp_folder"
cd "$temp_folder"

for tf in $(python -m crawler.configuration crawler project_folder)/data/"${crawl_id}"/*.tgz; do
    rm -r raw_html
    zf=${tf%.tgz}.zip
    echo "Converting $tf"
    tar -zxf "$tf" && zip -Z "store" -r "$zf" "raw_html" && touch -r "$tf" "$zf" && rm "$tf"
    echo ""
done
