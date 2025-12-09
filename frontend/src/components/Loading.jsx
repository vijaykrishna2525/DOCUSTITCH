import React from 'react';

const Loading = ({ status, progress }) => {
  return (
    <div className="w-full max-w-4xl mx-auto mt-8">
      <div className="bg-white rounded-xl shadow-md p-8">
        <div className="flex flex-col items-center">
          {/* Spinner */}
          <div className="relative w-16 h-16 mb-6">
            <div className="absolute top-0 left-0 w-full h-full border-4 border-blue-200 rounded-full"></div>
            <div className="absolute top-0 left-0 w-full h-full border-4 border-blue-600 rounded-full border-t-transparent animate-spin"></div>
          </div>

          {/* Status Message */}
          <h3 className="text-xl font-semibold text-gray-800 mb-2">
            {status?.message || 'Processing...'}
          </h3>

          {/* Progress Bar */}
          {progress !== null && progress !== undefined && (
            <div className="w-full max-w-md mt-4">
              <div className="flex items-center justify-between text-sm text-gray-600 mb-2">
                <span>Progress</span>
                <span className="font-medium">{Math.round(progress)}%</span>
              </div>
              <div className="w-full h-3 bg-gray-200 rounded-full overflow-hidden">
                <div
                  className="h-full bg-blue-600 transition-all duration-300 ease-out"
                  style={{ width: `${progress}%` }}
                ></div>
              </div>
            </div>
          )}

          {/* Status Details */}
          {status?.status && (
            <div className="mt-4 text-sm text-gray-500">
              Status: <span className="font-medium capitalize">{status.status}</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Loading;
