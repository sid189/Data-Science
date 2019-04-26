require(fpp)

plot(elecequip) #Q1: There are unusual patterns in the elecequip data

dec = stl(elecequip, s.window = "periodic") #Q2 The decomposition shows that the data is seasonal
dec
plot(dec)

newelec = seasadj(dec) #Q3 The plot of the seasonal data has been created 
plot(newelec)

x=BoxCox.lambda(newelec) #Q4 The plot is similar to that generated with the seasonally adjusted data in Q3 
newx=BoxCox(newelec, x)

acf(newelec) #Q5 The resulting data is not stationary
adf.test(newelec)

diffelec=ndiffs(newelec) # Q6 Since the resulting data is not stationary, we need to take the first difference of the data

elec2=diff(data2, differences = diffelec)
acf(elec2)
adf.test(elec2) #The resulting data is stationary

d=auto.arima(elec2) 
summary(d) #Q7 p=2 q=0 d=1

a1=Arima(elec2, order=c(4,0,0)) #Q8 The ARIMA models have been generated 
a2=Arima(elec2, order=c(3,0,0))
a3=Arima(elec2, order=c(2,0,0))

d$aicc #Since d$aicc has the lowest value, this model gives a better AIC
a1$aicc
a2$aicc
a3$aicc

r=residuals(d) #Q9 They do behave like white noise. Null hypothesis cannot be rejected
Acf(r)
Box.test(r)

plot(forecast(d)) #Q10 The model is proper

