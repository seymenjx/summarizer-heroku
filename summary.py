import aiohttp
import asyncio
import os
import ssl
from dotenv import load_dotenv
from summarizer.s3_handler import get_s3_files, get_file_content

load_dotenv()

TOGETHER_API_KEY = os.getenv('TOGETHER_API_KEY')
MODEL = os.getenv('MODEL', 'togethercomputer/llama-2-70b-chat')

# Create a custom SSL context that doesn't verify certificates
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

async def summarize_text(text):
    prompt = f"""Aşağıdaki metin bir Yargıtay kararıdır. Bu kararın özetini çıkarın ve kesinlikle aşağıdaki formatta sunun:

                Dava Konusu: [Davanın ana konusu ve taraflar arasındaki uyuşmazlığın özü]
                Hukuki Dayanak: [Kararın dayandığı kanun maddeleri, ilgili hukuki düzenlemeler ve içtihatlar]
                Mahkeme Kararı: [Mahkemenin vardığı nihai sonuç ve hüküm]
                Kararın Gerekçesi: [Mahkemenin gerekçesi ve karara varırken kullandığı hukuki ve somut değerlendirmeler]

                Lütfen her bölümü ayrı ayrı doldurun ve bölüm başlıklarını aynen kullanın. Örnek:

                Dava Konusu: X şirketi ile Y kurumu arasındaki vergi uyuşmazlığı
                Hukuki Dayanak: 213 sayılı Vergi Usul Kanunu'nun 378. maddesi
                Mahkeme Kararı: Davanın kabulüne karar verilmiştir
                Kararın Gerekçesi: Vergi mahkemesinin kararı usul ve yasaya uygun bulunmuştur

                Karar metni:
                {text}

                Özet:"""
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
            async with session.post(
            "https://api.together.xyz/inference",
            headers={
                "Authorization": f"Bearer {TOGETHER_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": MODEL,
                "prompt": prompt,
                "max_tokens": 2000,
                "temperature": 0.7,
                "top_p": 0.95,
                "top_k": 50,
                "repetition_penalty": 1.1,
                "stop": ["Summary:", "\n\n"]
            }
        ) as response:
                result = await response.json()
            return result['output']['choices'][0]['text'].strip()
    except Exception as e:
        print(f"Error in summarize_text: {str(e)}")
        return None

async def process_file(bucket_name, key, session):
    content = await get_file_content(bucket_name, key)
    summary = await summarize_text(content)
    return key, summary

async def summarize_files_from_s3(bucket_name, prefix='', max_files=100):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        tasks = []
        async for key in get_s3_files(bucket_name, prefix, max_files):
            tasks.append(process_file(bucket_name, key, session))
        results = await asyncio.gather(*tasks)
    return dict(results)

text = """
ESAS NO	: 2016/352 
KARAR NO	: 2018/170
DAVA	: Kooperatif  Genel Kurulu Kararının İptali
DAVA TARİHİ	: 20/07/2016
KARAR TARİHİ	: 14/02/2018
GEREKÇELİ KARARIN 
YAZILDIĞI TARİH                : 26/02/2018
Mahkememizde görülmekte olan Kooperatif Genel Kurulu Kararının İptali  davasının yapılan açık yargılaması sonunda,
GEREĞİ DÜŞÜNÜLDÜ:
Davacı vekili, müvekkilinin ortağı bulunduğu, davalı kooperatifin  20/06/2015 tarihinde yapılan genel kurul toplantısında alınan bir kısım kararların yasaya ve usule aykırı olması, aranan oy nisabının sağlanamaması nedeniyle hesap tetkik komisyonu kurulması, seçim, bütçe ile ilgili kararların iptaline karar verilmesini talep ve dava etmiştir.
Davalı vekili, davanın hak düşürücü süre olan 1 ay içerisinde açılmadığını, alınan kararların usul ve yasaya aykırı olmadığını belirterek davanın reddini karar verilmesini istemiştir.
Dava, Kooperatif  Genel Kurulu Kararının İptali isteğine ilişkindir.
Tarafların iddia ve savunmaları, sunulan ve sağlanan bilgi ve belgeler, bilirkişi raporu ve tüm dosya kapsamından anlaşılacağı üzere;
Davacının üyesi olduğu kooperatifin 20/06/2015 tarihinde yapılan genel kurul toplantısında alınan bir kısım kararların yasaya ve usule aykırı olduğu gerekçesiyle iş bu davayı açtığı anlaşılmaktadır.
Davacı iş bu davayı 1163 sayılı K.K. gereğince toplantı tarihini izleyen  bir aylık hak düşürücü süre içinde  süresinde açtığı saptanmıştır.
HÜKÜM: Yukarıda açıklandığı üzere;
1-Davanın kısmen kabulü ile  davalı kooperatifin 20/06/2015 tarihli olağan genel kurulunda  gündemin   9. maddesi gereğince alınan   aylık  %2  oranında gecikme faizi alınmasına ilişkin kararın  ve  gündemin 5. maddesiyle alınan ...'ün Yönetim kurulu asil üyeliğine seçilmesine ilişkin kararın bu kişi ile sınırlı olarak hükümsüz olduğunun tespitine, diğer iptal taleplerinin reddine,
2-Alınması gereken 35,90 TL karar harcından daha önce yatırılan 27,70 TL peşin harcın mahsubu ile bakiye  karar harcının davalıdan tahsiline,
3-Yürürlükteki AAÜT gereğince hesap edilen 2.180,00 TL vekalet ücretinin davalıdan alınıp davacıya verilmesine,
4-Yürürlükteki AAÜT gereğince hesap edilen 2.180,00 TL vekalet ücretinin davacıdan alınıp davalıya verilmesine,
5- Davacı tarafından yatırılan 27,70 TL başvuru harcı ile 37,70 TL peşin harcın davalıdan alınıp davacıya verilmesine,
6-Davacı tarafından yapılan ve aşağıda dökümü yapılan 650,00 TL yargılama giderinin davalıdan alınıp davacıya verilmesine,
7-Kullanılmayan gider avansı konusunda HMK 333. maddesi gereğince kararın kesinleşmesinden sonra karar verilmesine,
Dair, 6100 sayılı Hukuk Muhakemeleri Kanununun 341vd. maddeleri gereğince (5235 sayılı Kanunun 2. maddesi de dikkate alınarak) gerekçeli kararın tebliğinden itibaren iki hafta içinde mahkememize verilecek veya  başka bir mahkeme aracılığıyla gönderilecek dilekçe ile İstanbul Bölge Adliye Mahkemesi ilgili Hukuk Dairesi nezdinde istinaf kanun yolu açık olmak üzere hazır taraf vekillerinin yüzünde oy birliği ile verilen karar açıkça okunup anlatıldı. 14/02/2018           12:08 
Başkan ...
Üye ...
Üye ...
Katip ...
                                                             YARGILAMA MASRAFLARI
                                                                                DAVACI
                                                                        7 Tebligat - 60,00 TL
                                                                    2 Müzekkere - 20,00 TL
                                                                      Tanık Ücreti- 70,00 TL
                                                               Bilirkişi Ücreti - 500,00 TL
                                                                          Toplam = 650,00 TL

"""

if __name__ == "__main__":
    print(asyncio.run(summarize_text(text=text)))