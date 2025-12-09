import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * Upload a file (PDF or XML)
 * @param {File} file - The file to upload
 * @returns {Promise} Response with upload_id
 */
export const uploadFile = async (file) => {
  const formData = new FormData();
  formData.append('file', file);

  const response = await axios.post(`${API_BASE_URL}/upload`, formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  });

  return response.data;
};

/**
 * Upload a document from URL
 * @param {string} url - URL to the document
 * @param {string} docType - 'pdf' or 'xml'
 * @returns {Promise} Response with upload_id
 */
export const uploadFromURL = async (url, docType) => {
  const response = await api.post('/upload-url', {
    url,
    doc_type: docType,
  });

  return response.data;
};

/**
 * Start processing a document
 * @param {string} uploadId - Upload ID from upload endpoint
 * @returns {Promise} Status response
 */
export const processDocument = async (uploadId) => {
  const response = await api.post('/process', {
    upload_id: uploadId,
  });

  return response.data;
};

/**
 * Get processing status
 * @param {string} uploadId - Upload ID
 * @returns {Promise} Status response
 */
export const getStatus = async (uploadId) => {
  const response = await api.get(`/status/${uploadId}`);
  return response.data;
};

/**
 * Get processing results
 * @param {string} uploadId - Upload ID
 * @returns {Promise} Results with sections and summary
 */
export const getResult = async (uploadId) => {
  const response = await api.get(`/result/${uploadId}`);
  return response.data;
};

/**
 * Poll status until processing is complete
 * @param {string} uploadId - Upload ID
 * @param {Function} onProgress - Callback for progress updates
 * @returns {Promise} Final result
 */
export const pollUntilComplete = async (uploadId, onProgress) => {
  const maxAttempts = 120; // 2 minutes with 1 second intervals
  let attempts = 0;

  while (attempts < maxAttempts) {
    const status = await getStatus(uploadId);

    if (onProgress) {
      onProgress(status);
    }

    if (status.status === 'completed') {
      // Get final results
      return await getResult(uploadId);
    }

    if (status.status === 'failed') {
      throw new Error(status.message || 'Processing failed');
    }

    // Wait 1 second before next poll
    await new Promise(resolve => setTimeout(resolve, 1000));
    attempts++;
  }

  throw new Error('Processing timeout');
};

export default api;
