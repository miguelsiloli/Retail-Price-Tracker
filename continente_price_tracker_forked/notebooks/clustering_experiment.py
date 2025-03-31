import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sentence_transformers import SentenceTransformer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.mixture import GaussianMixture

class ClusteringEvaluator:
    def __init__(self, products):
        """
        Initialize the clustering evaluator
        
        :param products: List of product names
        """
        stop_words = set(stopwords.words('portuguese'))
        
        # Preprocess product names (remove brand, lowercase, remove stop words)
        self.products = products["Product"].tolist()
        self.products = [p.replace('Pingo Doce', '').strip() for p in self.products]
        self.products = [p.replace('Continente', '').strip() for p in self.products]
        self.products = [
            ' '.join([word for word in word_tokenize(p.lower()) if word not in stop_words])
            for p in self.products
        ]
        
        # Initialize Sentence Transformer model
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Generate embeddings
        self.embeddings = self.model.encode(self.products)

    def _generate_embeddings(self):
        """
        Generate embeddings for product names using FastText
        
        :return: numpy array of embeddings
        """
        embeddings = []
        for product in self.products:
            # Get word vectors and average them for the product name
            words = product.lower().split()
            word_vecs = [self.fasttext_model.get_word_vector(word) for word in words]
            
            # Average word vectors if the product name contains multiple words
            if word_vecs:
                product_embedding = np.mean(word_vecs, axis=0)
            else:
                # Fallback to zero vector if no words found
                product_embedding = np.zeros(300)  # Assuming 300-dimensional vectors
            
            embeddings.append(product_embedding)
        
        return np.array(embeddings)

    
    def evaluate_clustering(self, max_clusters=60, min_clusters=10):
        """
        Evaluate clustering performance across different cluster numbers using GMM.
        
        :param max_clusters: Maximum number of clusters to test
        :param min_clusters: Minimum number of clusters to test
        :return: Tuple of (BIC scores, silhouette scores)
        """
        # Prepare lists to store results
        bic_scores = []
        silhouette_scores = []
        cluster_range = range(min_clusters, max_clusters + 1)

        print(len(self.embeddings))
        
        # Iterate through different cluster numbers
        for n_clusters in cluster_range:
            # Perform Gaussian Mixture Model clustering
            gmm = GaussianMixture(
                n_components=n_clusters, 
                random_state=42, 
                n_init=5, 
                covariance_type='full'
            )
            gmm.fit(self.embeddings)
            
            # Calculate Bayesian Information Criterion (BIC)
            bic_scores.append(gmm.bic(self.embeddings))
            
            # Calculate silhouette score
            try:
                silhouette_avg = silhouette_score(self.embeddings, gmm.predict(self.embeddings))
                silhouette_scores.append(silhouette_avg)
            except Exception as e:
                print(f"Error calculating silhouette score for {n_clusters} clusters: {e}")
                silhouette_scores.append(np.nan)
            
        return bic_scores, silhouette_scores, cluster_range
    
    def plot_clustering_metrics(self):
        """
        Create visualization of clustering metrics
        """
        # Evaluate clustering
        inertia_scores, silhouette_scores, cluster_range = self.evaluate_clustering()
        
        # Create a figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Elbow Method Plot
        ax1.plot(cluster_range, inertia_scores, marker='o')
        ax1.set_xlabel('Number of Clusters')
        ax1.set_ylabel('Inertia (Within-Cluster Sum of Squares)')
        ax1.set_title('Elbow Method for Optimal k')
        
        # Silhouette Score Plot
        ax2.plot(cluster_range, silhouette_scores, marker='o', color='green')
        ax2.set_xlabel('Number of Clusters')
        ax2.set_ylabel('Silhouette Score')
        ax2.set_title('Silhouette Score by Number of Clusters')
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig('clustering_evaluation.png')
        plt.close()
        
        # Print out key insights
        max_silhouette_index = np.argmax(silhouette_scores)
        optimal_clusters = cluster_range[max_silhouette_index]
        
        print(f"Optimal number of clusters based on Silhouette Score: {optimal_clusters}")
        print(f"Highest Silhouette Score: {silhouette_scores[max_silhouette_index]:.4f}")
        
        return {
            'inertia_scores': inertia_scores,
            'silhouette_scores': silhouette_scores,
            'cluster_range': cluster_range,
            'optimal_clusters': optimal_clusters
        }

# Example usage
data = pd.read_csv('structured_pingo_doce.csv')
data = data.rename(columns={'product_name': 'Product'})

# Example usage
evaluator = ClusteringEvaluator(data)
results = evaluator.plot_clustering_metrics()


"""

HOW TO IMPROVE:

- ADD NEW DIMENSIONS: weights, size, quantity, price
- CHANGE THE CLUTERING MECHANISM
"""