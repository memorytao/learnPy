from settrade_v2 import Investor

investor = Investor(
    app_id="yB7fhQPMw4VWrAdA",
    app_secret="dh3mPCoty1lPUh/zlYR2n6gMhcjdyTEYuIpF4hgOpfM=",
    broker_id="SANDBOX",
    app_code="SANDBOX",
    is_auto_queue=False
)


deri = investor.Derivatives(account_no="Your Account No") 
account_info = deri.get_account_info()

print(account_info)