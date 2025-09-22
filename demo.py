from transformers import pipeline

summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

text = """Several free and open-source LLMs (Large Language Models) in 2025 excel at both summarization and text generation, offering capabilities for local or cloud deployment depending on specific needs and constraints.

Top Free and Open-Source LLMs for Summarization & Text Generation
Meta LLaMA 3 and LLaMA 2: Popular for robust summarization, abstractive capabilities, and text generation—both models are open source under the Meta community license. LLaMA 3 offers parameter sizes suitable for various resource needs and is optimized for dialogue and summarization tasks.

Mistral/Mixtral: Lightweight, privacy-friendly, and high-performing for both summarization and generation. Favored in developer and research communities, especially where on-premises/private deployments are required.

Falcon: Developed by the Technology Innovation Institute, known for efficient, scalable, and multilingual summarization and generation; suitable for real-time use.

Vicuna: Fine-tuned from LLaMA, recognized for interactive/conversational summarization and open-source flexibility, especially in local use cases.

GPT-NeoX-20B and GPT-J-6B (EleutherAI): Strong open-source alternatives to GPT-3, suitable for both text generation and summarization, with versions optimized for English and multilingual tasks.

BERT/BERTSum: Specializes in extractive summarization—precise, factual sentence extraction rather than full generative summaries. Remains a strong option for simple, high-accuracy summarization needs.

OpenChat, Zephyr: Popular fine-tuned models for domain-specific generative and summarization tasks, easily customizable for specific pipelines.

Choosing a Model
For local, privacy-compliant deployments: LLaMA 2/3, Mistral, Falcon, Vicuna.

For large-scale, multilingual, or high-efficiency summarization: Falcon, GPT-NeoX.

For basic extractive summarization: BERT/BERTSum.

For community-powered flexibility or rapid customization: OpenChat, Zephyr.

These models are widely used for internal tools, chatbots, document summarization, and research applications, and have easy integration paths via Hugging Face or similar platforms.

"""

# Generate summary
summary = summarizer(text, max_length=150, min_length=40, do_sample=False)
print(summary[0]['summary_text'])