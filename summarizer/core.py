import logging
import os
import aiohttp
import json
import ssl
import certifi
from tenacity import retry, stop_after_attempt, wait_random_exponential
from .s3_handler import get_s3_files, get_file_content, check_summary_exists, upload_summary_to_s3
import traceback
from dotenv import load_dotenv
import asyncio
load_dotenv()

logger = logging.getLogger(__name__)

# Set your Together AI API key
API_KEY = os.getenv("TOGETHER_API_KEY")
API_URL = "https://api.together.xyz/inference"

# Create SSL context
ssl_context = ssl.create_default_context(cafile=certifi.where())

@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(5))
async def summarize_text(text):
    if len(text) < 50:  # Add a check for very short texts
        return "Metin özet için çok kısa"

    try:
        prompt = f"""Aşağıdaki metin bir Yargıtay kararıdır. Bu kararın özetini çıkarın ve kesinlikle aşağıdaki formatta sunun:
                    Karar metni:
                    {text} 

                'Dava Konusu':' Davanın ana konusu ve taraflar arasındaki uyuşmazlık net bir şekilde ifade edilmelidir. Örneğin, "Bir iş sözleşmesinin feshi ile ilgili tazminat talebi" veya "Miras paylaşımı sırasında ortaya çıkan mal varlığı uyuşmazlığı" gibi. Bu bölümde davanın hangi hukuki alanla ilgili olduğu ve ne tür bir talebin incelendiği açıklanmalıdır.'
                'Hukuki Dayanak': 'Mahkemenin kararını dayandırdığı kanun maddeleri, ilgili hukuki düzenlemeler ve daha önceki içtihatlar bu bölümde belirtilmelidir. Örneğin, "6098 sayılı Türk Borçlar Kanunu'nun 123. maddesi" veya "Yargıtay 9. Hukuk Dairesi'nin emsal niteliğindeki kararı" gibi detaylar yer almalıdır.'
                'Mahkeme Kararı': 'Mahkemenin vardığı nihai sonuç ve verdiği hüküm burada belirtilir. Örneğin, "Davacının tazminat talebi kısmen kabul edilmiştir" veya "Mahkeme, davalının itirazını reddetmiştir" gibi karar özetlenir.'
                'Kararın Gerekçesi': 'Mahkemenin verdiği kararı hangi somut ve hukuki gerekçelerle desteklediği burada açıklanır. Örneğin, "Mahkeme, iş sözleşmesinin haklı bir nedenle feshedilmediği kanaatine varmıştır" gibi gerekçelere yer verilmelidir.'
                Eğer metinde bir bölüm için yeterli bilgi yoksa, "Bilgi bulunamadı" şeklinde not düşebilirsiniz.
                Lütfen her bölümü ayrı ayrı doldurun ve bölüm başlıklarını aynen kullanın. Önemli hukuki terimleri ve kanun numaralarını mutlaka belirtin. Özet kısa ve öz olmalı ve Turkce olarak yazilmali, ancak kritik bilgileri içermelidir. Eğer herhangi bir bölüm için bilgi bulunamazsa, o bölümü 'Bilgi bulunamadı' olarak işaretleyin. Lütfen cevabınızı sadece bu dört bölümle sınırlı tutun ve ekstra bilgi eklemeyin. 
                * kullanmayın. Cevaplari tek satirda yazin.
                Cevap:"""
        
        payload = {
            "model": os.getenv('MODEL', 'togethercomputer/llama-3.1-70b-chat'),
            "prompt": prompt,
            "max_tokens": 1500,
            "temperature": 0.7,
            "top_p": 0.95,
            "top_k": 40,
            "repetition_penalty": 1.1,
            "stop": ['Human:', '\n\n'],
            "stream": False
        }
        
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
            async with session.post(API_URL, json=payload, headers=headers) as response:
                response_data = await response.json()
                
                if 'output' in response_data and 'choices' in response_data['output']:
                    summary = response_data['output']['choices'][0]['text']
                    logger.info("Summary created successfully.")
                    return summary.strip()
                else:
                    logger.warning("Summary creation failed: No output in response.")
                    return "Özet oluşturulamadı."
        
    except Exception as e:
        logger.error(f"Error in summarize_text: {str(e)}")
        logger.error(traceback.format_exc())  # Log the full traceback
        return f"Özet oluşturulurken bir hata oluştu: {str(e)}"

def clean_summary(summary):
    if isinstance(summary, dict):
        cleaned_summary = {}
        for key, value in summary.items():
            if isinstance(value, str):
                sections = value.split('\n')
                cleaned_sections = []
                for section in sections:
                    if section.strip() and (not cleaned_sections or section != cleaned_sections[-1]):
                        cleaned_sections.append(section)
                cleaned_summary[key] = '\n'.join(cleaned_sections)
            else:
                cleaned_summary[key] = value
        return cleaned_summary
    elif isinstance(summary, str):
        sections = summary.split('\n')
        cleaned_sections = []
        for section in sections:
            if section.strip() and (not cleaned_sections or section != cleaned_sections[-1]):
                cleaned_sections.append(section)
        return '\n'.join(cleaned_sections)
    else:
        return summary  # Return as-is if it's neither string nor dict

async def parse_summary(summary):
    sections = ["Dava Konusu:", "Hukuki Dayanak:", "Mahkeme Kararı:", "Kararın Gerekçesi:"]
    parsed = {section.strip(':'): "Bilgi bulunamadı." for section in sections}
    
    if isinstance(summary, dict):
        # Handle the case where summary is already a dictionary
        for key in parsed.keys():
            if key in summary:
                parsed[key] = summary[key]
        parsed['Output'] = summary.get('Output', '')
    elif isinstance(summary, str):
        # Parse the string summary
        current_section = None
        lines = summary.split('\n')
        for line in lines:
            line = line.strip()
            if any(section in line for section in sections):
                current_section = line.split(':')[0].strip() + ':'
                parsed[current_section.strip(':')] = line.split(':', 1)[1].strip()
            elif current_section and line:
                parsed[current_section.strip(':')] += " " + line
        parsed['Output'] = summary
    else:
        logger.error(f"Unexpected summary type: {type(summary)}")
        parsed['Output'] = str(summary)

    # Clean up any remaining "Bilgi bulunamadı." entries if we have actual content
    for key, value in parsed.items():
        if value.strip() == "Bilgi bulunamadı." and parsed['Output']:
            parsed[key] = "Özet metninde bu bölüm için spesifik bilgi bulunamadı."

    # Ensure that the Tam Ozet Metni is properly filled
    parsed["Tam Ozet Metni"] = "\n".join([f"{section}: {content}" for section, content in parsed.items() if section != "Tam Ozet Metni" and section != "Output"])
    
    return parsed

async def run_summarize_files_from_s3(bucket_name, prefix, max_files):
    summarized_files = 0
    summaries = []
    logger.info(f"Starting to process files from bucket: {bucket_name}, prefix: {prefix}")
    async for file_key in get_s3_files(bucket_name, prefix, max_files):
        logger.info(f"Processing file: {file_key}")
        try:
            content = await get_file_content(bucket_name, file_key)
            logger.info(f"File content retrieved for: {file_key}")
            # Add your summarization logic here
            summary = "Sample summary"  # Replace with actual summarization
            logger.info(f"Summary generated for: {file_key}")
            
            # Save the summary to S3
            summary_key = f"{prefix}summarizer/{os.path.basename(file_key)}_summary.txt"
            # Implement upload_summary_to_s3 function
            # await upload_summary_to_s3(bucket_name, summary_key, summary)
            logger.info(f"Summary uploaded to S3 for: {file_key}")
            
            summaries.append({"file": file_key, "summary": summary})
            summarized_files += 1
            logger.info(f"Summarized file {file_key} ({summarized_files}/{max_files})")
            
            if summarized_files >= max_files:
                logger.info(f"Reached max_files limit of {max_files}")
                break
        except Exception as e:
            logger.error(f"Error processing file {file_key}: {str(e)}")
            continue

    logger.info(f"Completed processing. Total files summarized: {summarized_files}")
    return {"summarized_files": summarized_files, "summaries": summaries}
