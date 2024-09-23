import logging
import os
import aiohttp
import json
import ssl
import certifi
from tenacity import retry, stop_after_attempt, wait_random_exponential
from .s3_handler import get_s3_files, get_file_content
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
    logger.info(f"Starting summarization for text of length: {len(text)}")
    
    if len(text) < 50:  # Add a check for very short texts
        logger.warning(f"Text is too short (length: {len(text)}). Skipping summarization.")
        return "Metin özet için çok kısa"

    try:
        prompt = f"""Aşağıdaki metin bir Yargıtay kararıdır. Bu kararın özetini çıkarın ve kesinlikle aşağıdaki formatta sunun:

                Dava Konusu: [Davanın ana konusu ve taraflar arasındaki uyuşmazlığın özü]
                Hukuki Dayanak: [Kararın dayandığı kanun maddeleri, ilgili hukuki düzenlemeler ve içtihatlar]
                Mahkeme Kararı: [Mahkemenin vardığı nihai sonuç ve hüküm]
                Kararın Gerekçesi: [Mahkemenin gerekçesi ve karara varırken kullandığı hukuki ve somut değerlendirmeler]

                Lütfen her bölümü ayrı ayrı doldurun ve bölüm başlıklarını aynen kullanın. Önemli hukuki terimleri ve kanun numaralarını mutlaka belirtin. Özet kısa ve öz olmalı, ancak kritik bilgileri içermelidir. Eğer herhangi bir bölüm için bilgi bulunamazsa, o bölümü 'Bilgi bulunamadı' olarak işaretleyin. Lütfen cevabınızı sadece bu dört bölümle sınırlı tutun ve ekstra bilgi eklemeyin. Eğer metinde yeterli bilgi yoksa, 'Bilgi bulunamadı' yazmak yerine mevcut bilgileri kullanarak en iyi tahmini yapın.

                Karar metni:
                {text[:1000]}  # Limit the text to 1000 characters for logging

                Özet:"""
        
        logger.info(f"Generated prompt: {prompt[:500]}...")  # Log the first 500 characters of the prompt
        
        payload = {
            "model": os.getenv('MODEL', 'togethercomputer/llama-3-70b-chat'),
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
        
        logger.info("Sending request to AI API")
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
            async with session.post(API_URL, json=payload, headers=headers) as response:
                logger.info(f"Received response with status: {response.status}")
                response_data = await response.json()
                logger.info(f"Raw API response: {response_data}")  # Log the entire response
                
                if 'output' in response_data and 'choices' in response_data['output']:
                    summary = response_data['output']['choices'][0]['text']
                    logger.info(f"Generated summary: {summary[:500]}...")  # Log the first 500 characters of the summary
                    return summary.strip()
                else:
                    logger.error(f"Unexpected response format: {response_data}")
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
                    if section not in cleaned_sections[-3:]:
                        cleaned_sections.append(section)
                cleaned_summary[key] = '\n'.join(cleaned_sections)
            else:
                cleaned_summary[key] = value
        return cleaned_summary
    elif isinstance(summary, str):
        sections = summary.split('\n')
        cleaned_sections = []
        for section in sections:
            if section not in cleaned_sections[-3:]:
                cleaned_sections.append(section)
        return '\n'.join(cleaned_sections)
    else:
        return summary  # Return as-is if it's neither string nor dict

async def parse_summary(summary):
    logger.info(f"Parsing summary: {summary}")
    
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
    
    # If the Output is empty, fill it with the Tam Ozet Metni
    if not parsed['Output']:
        parsed['Output'] = parsed["Tam Ozet Metni"]
    
    return parsed

async def run_summarize_files_from_s3(bucket_name, prefix, max_files):
    try:
        file_count = 0
        async for key in get_s3_files(bucket_name, prefix, max_files):
            if file_count >= max_files:
                break
            content = await get_file_content(bucket_name, key)
            summary = await summarize_text(content)
            parsed_summary = await parse_summary(summary)
            yield {key: parsed_summary}
            file_count += 1
        logger.info("Summarization completed successfully")
    except Exception as e:
        logger.error(f"Error in run_summarize_files_from_s3: {str(e)}")
        logger.error(traceback.format_exc())
        raise
