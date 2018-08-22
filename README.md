# crawl.big.TW

### Configure Setting : 
Edit the file `bigTW/.env.example`, and save as `.env`
or You can :
```
cp .env.example .env
vim .env
```

### Python3 Requrement :
```
scrapy
pymysql
python-dovenv
```

### How to RUN :
* Move to project
```
cd bigTW
```
* RUN Scrapy 
```
scrapy crawl travel
```
* If you want to a `.csv` output file:
```
scrapy crawl travel -o output.csv
```
