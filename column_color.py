import pandas as pd


def highlight_max(s):
    '''
    highlight the maximum in a Series yellow.
    '''
    is_max = s > 40
    return ['background-color: yellow' if v else '' for v in is_max]


data = {
    "calories": [420, 380, 390],
    "duration": [50, 40, 99]
}


df = pd.DataFrame(data)

df.style.set_properties(**{'background-color': 'black'})

print(df.to_excel("test.xlsx", index=False))
