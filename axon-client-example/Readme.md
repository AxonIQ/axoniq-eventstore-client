Create bank account

```
$ echo '{"overdraftLimit":100}' | http :8081/bank-accounts
```

Deposit on the bank account 

```
echo '{"amount":55}' | http post :8081/bank-accounts/9fcd289d-80e0-4895-8f35-532881f6b2ca/deposits
```

Retrieve bank account

```
http :8081/bank-accounts/9fcd289d-80e0-4895-8f35-532881f6b2ca
```