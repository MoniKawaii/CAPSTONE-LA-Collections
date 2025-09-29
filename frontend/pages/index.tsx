import { useState, ChangeEvent } from "react";

type Platform = "Lazada" | "Shopee";

interface DataFrameData {
  message: string;
  rows_processed: number;
  columns: string[];
  data: Record<string, any>[];
  dataframe_shape: [number, number];
}

export default function UploadPage() {
  const [file, setFile] = useState<File | null>(null);
  const [platform, setPlatform] = useState<Platform>("Lazada");
  const [status, setStatus] = useState<string>("");
  const [dataFrameData, setDataFrameData] = useState<DataFrameData | null>(null);

  // Handle file selection
  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    setFile(e.target.files?.[0] || null);
  };

  // Handle platform selection
  const handlePlatformChange = (e: ChangeEvent<HTMLSelectElement>) => {
    setPlatform(e.target.value as Platform);
  };

  // Handle clear data
  const handleClear = () => {
    setDataFrameData(null);
    setStatus("");
    setFile(null);
  };

  // Handle file upload
  const handleUpload = async () => {
    if (!file) {
      setStatus("Please select a file first");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);
    formData.append("platform", platform);
    setStatus("Processing...");

    try {
      const res = await fetch("http://localhost:8000/upload", {
        method: "POST",
        body: formData,
      });

      if (res.ok) {
        const data = await res.json();
        setStatus(`File processed successfully! Processed: ${data.rows_processed || 0} rows. Columns: ${data.columns?.length || 0}`);
        setDataFrameData(data);
      } else {
        setStatus("Processing failed");
        setDataFrameData(null);
      }
    } catch (err) {
      console.error(err);
      setStatus("Error connecting to server");
      setDataFrameData(null);
    }
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-50 p-6">
      <div className="bg-white shadow-lg rounded-2xl p-6 w-full max-w-md text-black">
        <h1 className="text-xl font-semibold mb-4 text-center">Process CSV</h1>

        <label className="block mb-2">Select Platform:</label>
        <select
          value={platform}
          onChange={handlePlatformChange}
          className="mb-4 w-full border border-gray-300 rounded-lg p-2"
        >
          <option value="Lazada">Lazada</option>
          <option value="Shopee">Shopee</option>
        </select>

        <input
          type="file"
          accept=".csv"
          onChange={handleFileChange}
          className="mb-4 w-full border border-gray-300 rounded-lg p-2"
        />

        <button
          onClick={handleUpload}
          className="w-full bg-blue-500 text-white py-2 px-4 rounded-lg hover:bg-blue-600 transition mb-2"
        >
          Process CSV
        </button>

        {dataFrameData && (
          <button
            onClick={handleClear}
            className="w-full bg-gray-500 text-white py-2 px-4 rounded-lg hover:bg-gray-600 transition"
          >
            Clear Results
          </button>
        )}

        {status && <p className="mt-4 text-center text-black">{status}</p>}
      </div>

      {/* DataFrame Display */}
      {dataFrameData && (
        <div className="mt-8 w-full max-w-6xl bg-white shadow-lg rounded-2xl p-6">
          <h2 className="text-xl font-semibold mb-4 text-black">DataFrame Results</h2>
          <div className="mb-4 text-black">
            <p><strong>Shape:</strong> {dataFrameData.dataframe_shape[0]} rows × {dataFrameData.dataframe_shape[1]} columns</p>
            <p><strong>Platform:</strong> {platform}</p>
          </div>
          
          <div className="overflow-x-auto max-h-96 overflow-y-auto">
            <table className="min-w-full border-collapse border border-gray-300">
              <thead className="sticky top-0 bg-white">
                <tr className="bg-gray-100">
                  {dataFrameData.columns.map((column, index) => (
                    <th key={index} className="border border-gray-300 px-4 py-2 text-left text-black font-semibold text-sm">
                      {column}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataFrameData.data.map((row, rowIndex) => (
                  <tr key={rowIndex} className={rowIndex % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                    {dataFrameData.columns.map((column, colIndex) => (
                      <td key={colIndex} className="border border-gray-300 px-4 py-2 text-black text-sm">
                        {row[column] !== null && row[column] !== undefined 
                          ? (typeof row[column] === 'number' 
                             ? (row[column] % 1 === 0 ? row[column].toLocaleString() : row[column].toFixed(4))
                             : String(row[column]))
                          : 'N/A'
                        }
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
