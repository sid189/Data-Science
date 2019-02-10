library(reshape)
library(fastDummies)
library(dplyr)
library(qcc)
library(reshape2)
library(dummies)

mergetable <- function (pivottable) {
  diff=0.05
  pivottable['merge']=pivottable[1]
  len = dim(pivottable)[1]
  for (i in 1:(len-1)) {
    for (i1 in (i+1):len) {
      if ( i1 <=len) {
        if(abs(pivottable[i,2]-pivottable[i1,2]) < diff) {
          pivottable[i1, 3] = pivottable[i, 3]
        }
      }
    }
  }
  return(pivottable)
}

data <- read.csv('C:/Users/sidde/OneDrive/Documents/eBayAuctions.csv')
train <- sample_frac(data, 0.6)
sample <- as.numeric(rownames(train))
test <- data[-sample,]
colnames(train)[1] <- 'Category'
colnames(test)[1] <- 'Category'
ip.melt <- melt(train, id.vars=c(1,2,4,5), measure.vars=c(8))
cattable <- cast(ip.melt, Category ~ variable, mean)
curtable <- cast(ip.melt, currency ~ variable, mean)

edtable <- cast(ip.melt, endDay ~ variable, mean)
durtable <- cast(ip.melt, Duration ~ variable, mean)

cur <- mergetable(curtable)
len <- dim(cur)[1]
for (i in 1:len) {
  train[train['currency']%in%cur[i, 1], 'currency'] = cur[i, 3]
  test[test['currency']%in%cur[i, 1], 'currency'] = cur[i, 3]
}

cat <- mergetable(cattable)
len <- dim(cat)[1]
for (i in 1:len) {
  train[train["Category"]%in%cat[i, 1], "Category"] = cat[i, 3]
  test[test["Category"]%in%cat[i, 1], "Category"] = cat[i, 3]
}

dur <- mergetable(durtable)
len <- dim(dur)[1]
for (i in 1:len) {
  train[train['Duration']%in%dur[i, 1], 'Duration'] = dur[i, 3]
  test[test['Duration']%in%dur[i, 1], 'Duration'] = dur[i, 3]
}

ed <- mergetable(edtable)
len <- dim(ed)[1]
for (i in 1:len) {
  train[train['endDay']%in%ed[i, 1], 'endDay'] = ed[i, 3]
  test[test['endDay']%in%ed[i, 1], 'endDay'] = ed[i, 3]
}

train = dummy.data.frame(train)

fit.all <- glm(Competitive~., family=binomial(link='logit'), data=train,control=list(maxit=500))
coeff <- fit.all$coefficients

high <- coeff[1]
high1 <- 1
for (i in 2:length(coeff)) {
  diff1 = abs(as.numeric(coeff[i]))
  if(!is.na(diff1) && diff1 > abs(as.numeric((high)))) {
    high = coeff[i]
    high1=i
  }
}
high_name<-high
for (name in names(train1)) {
  if(grepl(name, names(fit.all$coefficients)[high1]))
    high_name = name
}
print(high_name)
sub <- c('Competitive', high_name)

fit.single <- glm(Competitive~., family=binomial(link = 'logit'), data = train1[sub])

sig <- 0.05

coeff <- summary(fit.all)$coefficients

sigpreds <- coeff[coeff[,4] < sig,]

high_name <- c("Competitive")
for (name in names(train1)) {
  for (sig in names(sigpreds[,1])) {
    if(grepl(name, sig)) {
      high_name = c(high_name, name)
    }
  }
}
high_name<-unique(high_name)

fit.reduced <- glm(Competitive~., family=binomial(link = 'logit'), data=train1[high_name])

anova(fit.reduced, fit.all, test='Chisq')

over<-rep(length(train1$'Competitive'), length(train1$'Competitive'))
qcc.overdispersion.test(train1$'Competitive', size=over, type='binomial')

