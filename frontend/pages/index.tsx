import { useState, ChangeEvent } from "react";

type Platform = "Lazada" | "Shopee";

export default function UploadPage() {
  const [file, setFile] = useState<File | null>(null);
  const [platform, setPlatform] = useState<Platform>("Lazada");
  const [status, setStatus] = useState<string>("");

  // Handle file selection
  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    setFile(e.target.files?.[0] || null);
  };

  // Handle platform selection
  const handlePlatformChange = (e: ChangeEvent<HTMLSelectElement>) => {
    setPlatform(e.target.value as Platform);
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
    setStatus("Uploading...");

    try {
      const res = await fetch("http://localhost:8000/upload", {
        method: "POST",
        body: formData,
      });

      if (res.ok) {
        const data = await res.json();
        setStatus(`File uploaded successfully! Inserted: ${data.inserted || 0}`);
      } else {
        setStatus("Upload failed");
      }
    } catch (err) {
      console.error(err);
      setStatus("Error connecting to server");
    }
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-50 p-6">
      <div className="bg-white shadow-lg rounded-2xl p-6 w-full max-w-md text-black">
        <h1 className="text-xl font-semibold mb-4 text-center">Upload CSV</h1>

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
          className="w-full bg-blue-500 text-white py-2 px-4 rounded-lg hover:bg-blue-600 transition"
        >
          Upload
        </button>

        {status && <p className="mt-4 text-center text-black">{status}</p>}
      </div>
    </div>
  );
}
