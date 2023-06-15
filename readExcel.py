from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
# from office365.sharepoint.lists.list import Lists


site_url = 'https://telenorgroup.sharepoint.com/:x:/r/sites/NewCMPPelatroworkingTeamDTAC/Shared%20Documents/General/Postpaid%20Campaign/NBA_Configuration_FIle/Postpaid_NBA_State_configure_1.1_paletro_20230323.xlsx?d=w387951a0e38946f1b684fe19c230a7e3&csf=1&web=1&e=aCjF1o'
ctx_auth = AuthenticationContext(site_url)
ctx_auth.acquire_token_for_user('T968672', 'Taohowcome2023!')

ctx = ClientContext(site_url, ctx_auth)

list_title = 'Version History'
sp_list = ctx.web.lists.get_by_title(list_title)
ctx.load(sp_list)
ctx.execute_query()