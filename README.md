---
layout: post
title: Predicting Subway Usage in the Covid-19 Era
cover-img: /assets/img/PennStat.png
#thumbnail-img: /assets/img/thumb.png
#share-img: /assets/img/path.jpg
tags: [Forecasting, R, Python]
---

# Summary:
The Covid-19 pandemic has presented many industries with an interesting problem, Is it possible to model customer actions as they begin to adjust to the new normal? Using Data provided by New York's Metropolitan Transportation Authority, I was able to create a reliable firecast. 

# Retrieving and Cleaning the Data
All data for this project can be found on Metropolitan Transportation Authority's (MTA) [website](http://web.mta.info/developers/turnstile.html) or on [my github page.](https://github.com/ricepc/MTA_Turnstile_Data) 

Each week the MTA posts a txt file with the daily number of entries and exits from all New York subway stations. My goal in this analysis was to focus on only the data from Penn Station to better understand what patterns exist in the data and how they may be used to predict future ridership as New Yorkers begin to adjust to the new normal.



# Visualizing the Data
With any time series project the first step is to simply plot the data vs time. Below we see just how big of an impact Covid-19 has had on travel through Penn Station. Prior to the Pandemic, a typical weekday could bring upwards of 300,000 people, and now a whole order of magnitude less. There also appears to be some seasonality within each week and across each year. Something we will want to explore before building our models.  

![Time Series Data](https://github.com/ricepc/ricepc.github.io/raw/master/assets/Turnstile%20_Images/Full_TS_AutoPlot.JPG)

For the sake of modeling I have chosen to only look at data starting from April 2020.  
![Covid Time Series Data](https://github.com/ricepc/ricepc.github.io/raw/master/assets/Turnstile%20_Images/Covid_TS_Autoplot.jpeg)

When looking at the data by weekday, itis apparent that there will be less traffic on weekends. This make senses considering commuters do not usually have to enter the city on these days. There are three data points, all on Mondays, that appear to have large deviations from the trend. These correspond with Memorial Day, Labor Day, and Columbus Day all of which mean less workers.
![Covid ggseason](https://github.com/ricepc/ricepc.github.io/raw/master/assets/Turnstile%20_Images/Covid_ggseason.png)

![Covid ssubseries](https://github.com/ricepc/ricepc.github.io/raw/master/assets/Turnstile%20_Images/covid_ggsubseries.png)

Using STL decomposition, we are able to break the data into its trend and seasonal componets. Penn Station's ridership has increasing fairly linearly since the intial weeks of the pandemic. The change in seasonality also corresponds with this increasing trend.
![Covid Decomp](https://github.com/ricepc/ricepc.github.io/raw/master/assets/Turnstile%20_Images/STL_Decomp.png)

# Forecast Models
There are three type of models I used on this data. ETS, ARIMA, and an ensable of the two. 

## ETS
## ARIMA
## Ensable Method
## Model Comparisons

# Conclusion
