# steo

This repo contains scripts to download the spreadsheets of the US short-term energy price forecasts by EIA (US Energy Information Administration) and evaluate the forecast performance. 

### To run 

Make sure `python version >= 3.7`

```
git clone https://github.com/JingweiZhang2017/steo.git

cd steo

pip install -r requirements.txt

python3 STEO.py
```

It will create 3 folder with csv files:

`/data` for forecast spreadsheets downloaded from https://www.eia.gov/outlooks/steo/outlook.php#issues2021

`/pred` for parsed tables with prediction from 1 to 12 month ahead

`/eval` for evaluation results with metric like MAE, RMSE and MAPE



