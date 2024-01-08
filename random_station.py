import random

# Create arrays with all stations
where_do_we_go =  {
"BTS_SUKHUMVIT_LINE": ['N24:คูคต | Khu Khot', 'N23:แยก คปอ. | Yaek Kor Por Aor', 'N22:พิพิธภัณฑ์กองทัพอากาศ | Royal Thai Air Force Museum', 'N21:โรงพยาบาลภูมิพลอดุลยเดช | Bhumibol Adulyadej Hospital', 'N20:สะพานใหม่ | Saphan Mai', 'N19:สายหยุด | Sai Yud', 'N18:พหลโยธิน 59 | Phahon Yothin 59', 'N17:วัดพระศรีมหาธาตุ | Wat Phra Sri Mahathat', 'N16:กรมทหารราบที่ 11 | 11th Infantry Regiment', 'N15:บางบัว | Bang Bua', 'N14:กรมป่าไม้ | Royal Forest Department', 'N13:มหาวิทยาลัยเกษตรศาสตร์ | Kasetsart University', 'N12:เสนานิคม | Sena Nikhom', 'N11:รัชโยธิน | Ratchayothin', 'N10:พหลโยธิน 24 | Phahon Yothin 24', 'N9:ห้าแยกลาดพร้าว | Ha Yaek Lat Phrao', 'N8:หมอชิต | Mo Chit', 'N7:สะพานควาย | Saphan Khwai', 'N5:อารีย์ | Ari', 'N4:สนามเป้า | Sanam Pao', 'N3:อนุสาวรีย์ชัยสมรภูมิ | Victory Monument', 'N2:พญาไท | Phaya Thai', 'N1:ราชเทวี | Ratchathewi', 'CEN:สยาม | Siam', 'E1:ชิดลม | Chit Lom', 'E2:เพลินจิต | Phloen Chit', 'E3:นานา | Nana', 'E4:อโศก | Asok', 'E5:พร้อมพงษ์ | Phrom Phong', 'E6:ทองหล่อ | Thong Lo', 'E7:เอกมัย | Ekkamai', 'E8:พระโขนง | Phra Khanong', 'E9:อ่อนนุช | On Nut', 'E10:บางจาก | Bang Chak', 'E11:ปุณณวิถี | Punnawithi', 'E12:อุดมสุข | Udom Suk', 'E13:บางนา | Bang Na', 'E14:แบริ่ง | Bearing', 'E15:สำโรง | Samrong', 'E16:ปู่เจ้า | Pu Chao', 'E17:ช้างเอราวัณ | Chang Erawan', 'E18:โรงเรียนนายเรือ | Royal Thai Naval Academy', 'E19:ปากน้ำ | Pak Nam', 'E20:ศรีนครินทร์ | Srinagarindra', 'E21:แพรกษา | Phraek Sa', 'E22:สายลวด | Sai Luat', 'E23:เคหะฯ | Kheha'],

"BTS_SILOM_LINE":['W1:สนามกีฬาแห่งชาติ | National Stadium', 'CEN:สยาม | Siam', 'S1:ราชดำริ | Ratchadamri', 'S2:ศาลาแดง | Sala Daeng', 'S3:ช่องนนทรี | Chong Nonsi', 'S4:เซนต์หลุยส์ | Saint Louis', 'S5:สุรศักดิ์ | Surasak', 'S6:สะพานตากสิน | Saphan Taksin', 'S7:กรุงธนบุรี | Krung Thon Buri', 'S8:วงเวียนใหญ่ | Wongwian Yai', 'S9:โพธิ์นิมิตร | Pho Nimit', 'S10:ตลาดพลู | Talat Phlu', 'S11:วุฒากาศ | Wutthakat', 'S12:บางหว้า | Bang Wa'],

"GOLD_LINE": ['G1:กรุงธนบุรี (Gold Line) | Krung Thon Buri (Gold Line)', 'G2:เจริญนคร | Charoen Nakhon', 'G3:คลองสาน | Khlong San'],

"YELLOW_LINE": ['YL01:ลาดพร้าว | Lat Phrao', 'YL02:ภาวนา | Phawana', 'YL03:โชคชัย 4 | Chok Chai 4', 'YL04:ลาดพร้าว 71 | Lat Phrao 71', 'YL05:ลาดพร้าว 83 | Lat Phrao 83', 'YL06:มหาดไทย | Mahat Thai', 'YL07:ลาดพร้าว 101 | Lat Phrao 101', 'YL08:บางกะปิ | Bang Kapi', 'YL09:แยกลำสาลี | Yaek Lam Sali', 'YL10:ศรีกรีฑา | Si Kritha', 'YL11:หัวหมาก | Hua Mak', 'YL12:กลันตัน | Kalantan', 'YL13:ศรีนุช | Si Nut', 'YL14:ศรีนครินทร์ 38 | Srinagarindra 38', 'YL15:สวนหลวง ร.9 | Suan Luang Rama IX', 'YL16:ศรีอุดม | Si Udom', 'YL17:ศรีเอี่ยม | Si lam', 'YL18:ศรีลาซาล | Si La Salle', 'YL19:ศรีแบริ่ง | Si Bearing', 'YL20:ศรีด่าน | Si Dan', 'YL21:ศรีเทพา | Si Thepha', 'YL22:ทิพวัล | Thipphawan', 'YL23:สำโรง | Samrong'],

"PINK_LINE": ['PK01:ศูนย์ราชการนนทบุรี | Nonthaburi Civic Center', 'PK02:แคราย | Khae Rai', 'PK03:สนามบินน้ำ | Sanambin Nam', 'PK04:สามัคคี | Samakkhi', 'PK05:กรมชลประทาน | Royal Irrigation Department', 'PK06:แยกปากเกร็ด | Yaek Pak Kret', 'PK07:เลี่ยงเมืองปากเกร็ด | Pak Kret Bypass', 'PK08:แจ้งวัฒนะ-ปากเกร็ด 28 | Chaeng Watthana-Pak Kret 28', 'PK09:ศรีรัช | Si Rat', 'PK10:เมืองทองธานี | Muang Thong Thani', 'PK11:แจ้งวัฒนะ 14 | Chaeng Watthana 14', 'PK12:ศูนย์ราชการเฉลิมพระเกียรติ | Government Complex', 'PK13:โทรคมนาคมแห่งชาติ | National Telecom', 'PK14:หลักสี่ | Lak Si', 'PK15:ราชภัฏพระนคร | Rajabhat Phranakhon', 'PK16:วัดพระศรีมหาธาตุ | Wat Phra Sri Mahathat', 'PK17:รามอินทรา 3 | Ram Inthra 3', 'PK18:ลาดปลาเค้า | Lat Pla Khao', 'PK19:รามอินทรา กม.4 | Ram Inthra Kor Mor 4', 'PK20:มัยลาภ | Maiyalap', 'PK21:วัชรพล | Vacharaphol', 'PK22:รามอินทรา กม.6 | Ram Inthra Kor Mor 6', 'PK23:คู้บอน | Khu Bon', 'PK24:รามอินทรา กม.9 | Ram Inthra Kor Mor 9', 'PK25:วงแหวนรามอินทรา | Outer Ring Road-Ram Inthra', 'PK26:นพรัตน์ | Nopparat', 'PK27:บางชัน | Bang Chan', 'PK28:เศรษฐบุตรบำเพ็ญ | Setthabutbamphen', 'PK29:ตลาดมีนบุรี | Min Buri Market', 'PK30:มีนบุรี | Min Buri'],

"MRT_STATIONS": [
   "Tha Phra (BL01)",
    "Charan 13 (BL02)",
    "Fai Chai (BL03)",
    "Bang Khun Non (BL04)",
    "Bang Yi Khan (BL05)",
    "Sirindhorn (BL06)",
    "Bang Phlat (BL07)",
    "Bang O (BL08)",
    "Bang Pho (BL09)",
    "Tao Poon (BL10)",
    "Bang Sue (BL11)",
    "Kamphaeng Phet (BL12)",
    "Chatuchak Park (BL13)",
    "Phahon Yothin (BL14)",
    "Lat Phrao (BL15)",
    "Ratchadaphisek (BL16)",
    "Sutthisan (BL17)",
    "Huai Khwang (BL18)",
    "Thailand Cultural Centre (BL19)",
    "Phra Ram 9 (BL20)",
    "Phetchaburi (BL21)",
    "Sukhumvit (BL22)",
    "Queen Sirikit National Convention Centre (BL23)",
    "Khlong Toei (BL24)",
    "Lumphini (BL25)",
    "Si Lom (BL26)",
    "Sam Yan (BL27)",
    "Hua Lamphong (BL28)",
    "Wat Mangkon (BL29)",
    "Sam Yot (BL30)",
    "Sanam Chai (BL31)",
    "Itsaraphap (BL32)",
    "Bang Phai (BL33)",
    "Bang Wa (BL34)",
    "Phetkasem 48 (BL35)",
    "Phasi Charoen (BL36)",
    "Bang Khae (BL37)",
    "Lak Song (BL38)"]
}

# Randomly BTS or MRT
bts_or_mrt = random.choice(list(where_do_we_go.keys()))

# List of stations
stations = list(where_do_we_go[bts_or_mrt])

print(f"Transport : {bts_or_mrt}")
print(f"Station : {random.choice(stations)}")