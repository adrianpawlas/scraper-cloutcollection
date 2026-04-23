"""
Embeddings module using SigLIP (google/siglip-base-patch16-384)
Generates 768-dimensional embeddings for both images and text
"""
import io
import math
from typing import Optional, Union
from PIL import Image
import requests
import torch
from transformers import AutoProcessor, AutoModel


class SigLIPEmbedder:
    """SigLIP embedder for both image and text embeddings"""
    
    def __init__(
        self,
        model_name: str = "google/siglip-base-patch16-384",
        device: Optional[str] = None,
        cache_dir: Optional[str] = None
    ):
        self.model_name = model_name
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
        self.cache_dir = cache_dir
        
        # Load model and processor on first use
        self.model = None
        self.processor = None
    
    def _ensure_loaded(self):
        """Lazy load model and processor"""
        if self.model is None:
            print(f"Loading SigLIP model: {self.model_name}")
            self.processor = AutoProcessor.from_pretrained(
                self.model_name,
                cache_dir=self.cache_dir
            )
            self.model = AutoModel.from_pretrained(
                self.model_name,
                cache_dir=self.cache_dir
            )
            self.model.to(self.device)
            self.model.eval()
            print(f"Model loaded on {self.device}")
    
    def embed_image(
        self,
        image: Union[str, Image.Image, bytes],
        normalize: bool = True
    ) -> torch.Tensor:
        """
        Generate image embedding from URL, PIL Image, or bytes
        
        Args:
            image: Image URL string, PIL Image, or image bytes
            normalize: Whether to normalize the embedding
        
        Returns:
            Tensor of shape (768,)
        """
        self._ensure_loaded()
        
        # Load image from different sources
        if isinstance(image, str):
            # URL - fetch the image
            image = self._load_image_from_url(image)
        elif isinstance(image, bytes):
            # Bytes - convert to PIL Image
            image = Image.open(io.BytesIO(image)).convert('RGB')
        
        if not isinstance(image, Image.Image):
            raise ValueError(f"Cannot process image of type {type(image)}")
        
        # Process image
        inputs = self.processor(
            images=image,
            return_tensors="pt"
        )
        
        # Move to device
        pixel_values = inputs['pixel_values'].to(self.device)
        
        # Generate embedding
        with torch.no_grad():
            outputs = self.model.get_image_features(pixel_values=pixel_values)
            
            if hasattr(outputs, 'logits'):
                embedding = outputs.logits
            elif hasattr(outputs, 'pooler_output'):
                embedding = outputs.pooler_output
            else:
                embedding = outputs[0] if isinstance(outputs, tuple) else outputs
        
        # Reshape if needed (batch processing returns different shape)
        if len(embedding.shape) > 1:
            embedding = embedding.squeeze(0)
        
        # Normalize if requested
        if normalize:
            embedding = torch.nn.functional.normalize(embedding, p=2, dim=0)
        
        return embedding.cpu()
    
    def embed_text(
        self,
        text: Union[str, list[str]],
        normalize: bool = True
    ) -> torch.Tensor:
        """
        Generate text embedding
        
        Args:
            text: Single string or list of strings
            normalize: Whether to normalize the embedding
        
        Returns:
            Tensor of shape (768,) for single text, or (n, 768) for list
        """
        self._ensure_loaded()
        
        # Handle single string
        is_single = isinstance(text, str)
        if is_single:
            text = [text]
        
# Process text
        inputs = self.processor(
            text=text,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=64  # SigLIP base has 64 token limit
        )

        # Move to device
        input_ids = inputs['input_ids'].to(self.device)

        # Generate embedding
        with torch.no_grad():
            outputs = self.model.get_text_features(input_ids=input_ids)
            
            # Handle different output types from SigLIP
            if hasattr(outputs, 'logits'):
                embedding = outputs.logits
            elif hasattr(outputs, 'pooler_output'):
                embedding = outputs.pooler_output
            else:
                # Try to extract embedding from the output
                embedding = outputs[0] if isinstance(outputs, tuple) else outputs

        # Apply temperature scaling (optional, helps with similarity)
        temperature = self.model.logit_scale.exp()
        embedding = embedding * temperature

        # Reshape if single text
        if is_single:
            embedding = embedding.squeeze(0)

        # Normalize if requested
        if normalize:
            embedding = torch.nn.functional.normalize(embedding, p=2, dim=-1)

        return embedding.cpu()
    
    def embed_images_batch(
        self,
        images: list,
        batch_size: int = 8,
        normalize: bool = True
    ) -> list[torch.Tensor]:
        """
        Generate embeddings for multiple images in batches
        
        Args:
            images: List of image URLs, PIL Images, or bytes
            batch_size: Number of images to process at once
            normalize: Whether to normalize embeddings
        
        Returns:
            List of embedding tensors
        """
        self._ensure_loaded()
        
        embeddings = []
        
        for i in range(0, len(images), batch_size):
            batch = images[i:i + batch_size]
            print(f"Processing image batch {i//batch_size + 1}/{(len(images) + batch_size - 1)//batch_size}")
            
            # Load images
            loaded_images = []
            for img in batch:
                if isinstance(img, str):
                    img = self._load_image_from_url(img)
                elif isinstance(img, bytes):
                    img = Image.open(io.BytesIO(img)).convert('RGB')
                loaded_images.append(img)
            
            # Process batch
            inputs = self.processor(
                images=loaded_images,
                return_tensors="pt"
            )
            
            pixel_values = inputs['pixel_values'].to(self.device)
            
            with torch.no_grad():
                outputs = self.model.get_image_features(pixel_values=pixel_values)
                batch_embeddings = outputs.logits
            
            # Normalize
            if normalize:
                batch_embeddings = torch.nn.functional.normalize(batch_embeddings, p=2, dim=-1)
            
            # Add to list
            for emb in batch_embeddings:
                embeddings.append(emb.cpu())
        
        return embeddings
    
    def embed_texts_batch(
        self,
        texts: list[str],
        batch_size: int = 16,
        normalize: bool = True
    ) -> list[torch.Tensor]:
        """
        Generate embeddings for multiple texts in batches
        """
        self._ensure_loaded()
        
        embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            inputs = self.processor(
                text=batch,
                return_tensors="pt",
                padding=True
            )
            
            input_ids = inputs['input_ids'].to(self.device)
            attention_mask = inputs['attention_mask'].to(self.device)
            
            with torch.no_grad():
                outputs = self.model.get_text_features(
                    input_ids=input_ids,
                    attention_mask=attention_mask
                )
                batch_embeddings = outputs.logits
            
            temperature = self.model.logit_scale.exp()
            batch_embeddings = batch_embeddings * temperature
            
            if normalize:
                batch_embeddings = torch.nn.functional.normalize(batch_embeddings, p=2, dim=-1)
            
            for emb in batch_embeddings:
                embeddings.append(emb.cpu())
        
        return embeddings
    
    def _load_image_from_url(self, url: str) -> Image.Image:
        """Load image from URL"""
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return Image.open(io.BytesIO(response.content)).convert('RGB')
    
    def to_numpy(self, tensor: torch.Tensor) -> list:
        """Convert tensor to list for database"""
        return tensor.tolist()
    
    def to_vector_string(self, tensor: torch.Tensor) -> str:
        """Convert tensor to PostgreSQL vector string format"""
        return '[' + ','.join(str(x) for x in tensor.tolist()) + ']'


def create_siglip_embedder(
    model_name: str = "google/siglip-base-patch16-384",
    device: Optional[str] = None
) -> SigLIPEmbedder:
    """Factory function to create SigLIP embedder"""
    return SigLIPEmbedder(model_name, device)