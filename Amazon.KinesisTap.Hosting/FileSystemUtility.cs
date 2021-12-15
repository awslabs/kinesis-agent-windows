using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using Microsoft.Win32.SafeHandles;
using NLog.Config;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Utility class for file-system functions.
    /// </summary>
    internal static class FileSystemUtility
    {
        /// <summary>
        /// Error code for file-not-found.
        /// </summary>
        private const int WIN32_ERROR_FILE_NOT_FOUND = 0x2;

        /// <summary>
        /// The filename, directory name, or volume label syntax is incorrect.
        /// </summary>
        private const int WIN32_ERROR_INVALID_NAME = 0x7B;

        /// <summary>
        /// The file or directory is not a reparse point.
        /// </summary>
        private const int WIN32_ERROR_NOT_A_REPARSE_POINT = 4390;

        /// <summary>
        /// Used for mount point and junction support.
        /// </summary>
        private const uint WIN32_IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003;

        /// <summary>
        /// Used for symbolic link support.
        /// </summary>
        private const uint WIN32_IO_REPARSE_TAG_SYMLINK = 0xA000000C;

        /// <summary>
        /// Retrieves the reparse point data associated with the specified file or directory.
        /// </summary>
        private const int WIN32_FSCTL_GET_REPARSE_POINT = 0x000900A8;

        /// <summary>
        /// This prefix indicates to NTFS that the path is to be treated as object in the physical device namespace.
        /// </summary>
        private const string DeviceNamespacePrefix = @"\??\";

        /// <summary>
        /// Indicate the type of reparse point attached to a directory object.
        /// </summary>
        public enum DirectoryReparseType : int
        {
            /// <summary>
            /// Unrecognized reparse tag.
            /// </summary>
            Unknown = -1,

            /// <summary>
            /// Not a reparse point.
            /// </summary>
            None = 0,

            /// <summary>
            /// Mount point and junction.
            /// </summary>
            MountPoint = 1,

            /// <summary>
            /// Symlink.
            /// </summary>
            Symlink = 2
        }

        [Flags]
        private enum FileAccessDword : uint
        {
            GenericRead = 0x80000000,
            GenericWrite = 0x40000000,
            GenericExecute = 0x20000000,
            GenericAll = 0x10000000
        }

        [Flags]
        private enum FileShareDword : uint
        {
            None = 0x00000000,
            Read = 0x00000001,
            Write = 0x00000002,
            Delete = 0x00000004
        }

        private enum CreationDispositionDword : uint
        {
            New = 1,
            CreateAlways = 2,
            OpenExisting = 3,
            OpenAlways = 4,
            TruncateExisting = 5
        }

        [Flags]
        private enum FileAttributesDword : uint
        {
            Readonly = 0x00000001,
            Hidden = 0x00000002,
            System = 0x00000004,
            Directory = 0x00000010,
            Archive = 0x00000020,
            Device = 0x00000040,
            Normal = 0x00000080,
            Temporary = 0x00000100,
            SparseFile = 0x00000200,
            ReparsePoint = 0x00000400,
            Compressed = 0x00000800,
            Offline = 0x00001000,
            NotContentIndexed = 0x00002000,
            Encrypted = 0x00004000,
            Write_Through = 0x80000000,
            Overlapped = 0x40000000,
            NoBuffering = 0x20000000,
            RandomAccess = 0x10000000,
            SequentialScan = 0x08000000,
            DeleteOnClose = 0x04000000,
            BackupSemantics = 0x02000000,
            PosixSemantics = 0x01000000,
            OpenReparsePoint = 0x00200000,
            OpenNoRecall = 0x00100000,
            FirstPipeInstance = 0x00080000
        }

        /// <summary>
        /// API binding CreateFile function (https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea).
        /// </summary>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern SafeFileHandle CreateFile(
            string lpFileName,
            FileAccessDword dwDesiredAccess,
            FileShareDword dwShareMode,
            IntPtr lpSecurityAttributes,
            CreationDispositionDword dwCreationDisposition,
            FileAttributesDword dwFlagsAndAttributes,
            IntPtr hTemplateFile);

        /// <summary>
        /// API binding for DeviceIoControl function (https://docs.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-deviceiocontrol?redirectedfrom=MSDN).
        /// </summary>
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern bool DeviceIoControl(IntPtr hDevice, uint dwIoControlCode,
            IntPtr lpInBuffer, int nInBufferSize,
            IntPtr lpOutBuffer, int nOutBufferSize,
            out int lpBytesReturned, IntPtr lpOverlapped);

        /// <summary>
        /// Struct to store reparse data per https://docs.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_reparse_data_buffer.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        private struct REPARSE_DATA_BUFFER
        {
            /// <summary>
            /// Reparse point tag. Must be a Microsoft reparse point tag.
            /// </summary>
            public uint ReparseTag;

            /// <summary>
            /// Size, in bytes, of the reparse data in the buffer that DataBuffer points to.
            /// </summary>
            public ushort ReparseDataLength;

            /// <summary>
            /// Length, in bytes, of the unparsed portion of the file name pointed to by the FileName member of the associated file object.
            /// </summary>
            public ushort Reserved;

            /// <summary>
            /// Offset, in bytes, of the substitute name string in the PathBuffer array.
            /// </summary>
            public ushort SubstituteNameOffset;

            /// <summary>
            /// Length, in bytes, of the substitute name string. If this string is null-terminated,
            /// SubstituteNameLength does not include space for the null character.
            /// </summary>
            public ushort SubstituteNameLength;

            /// <summary>
            /// Offset, in bytes, of the print name string in the PathBuffer array.
            /// </summary>
            public ushort PrintNameOffset;

            /// <summary>
            /// Length, in bytes, of the print name string. If this string is null-terminated,
            /// PrintNameLength does not include space for the null character. 
            /// </summary>
            public ushort PrintNameLength;

            /// <summary>
            /// A buffer containing the unicode-encoded path string.
            /// </summary>
            /// <remarks>
            /// The size of this buffer is set so that the entire struct has size MAXIMUM_REPARSE_DATA_BUFFER_SIZE (which is 16*1024).
            /// </remarks>
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 0x3FF0)]
            public byte[] PathBuffer;
        }

        /// <summary>
        /// Disable all NLog targets in the configuration.
        /// </summary>
        /// <param name="loggingConfiguration">NLog configuration.</param>
        public static void DisableAllNLogTargets(LoggingConfiguration loggingConfiguration)
        {
            foreach (var rule in loggingConfiguration.LoggingRules)
            {
                foreach (var level in NLog.LogLevel.AllLevels)
                {
                    rule.DisableLoggingForLevel(level);
                }
            }
        }

        /// <summary>
        /// Get the type of reparse point for a directory.
        /// </summary>
        /// <param name="path">Directory path.</param>
        /// <returns>Type of reparse point.</returns>
        [SupportedOSPlatform("windows")]
        public static DirectoryReparseType GetDirectoryReparseType(string path)
            => GetDirectoryReparseTypeInternal(path);

        private static DirectoryReparseType GetDirectoryReparseTypeInternal(string path)
        {
            using var handle = OpenDirHandle(path, FileAccessDword.GenericRead);
            var outBufferSize = Marshal.SizeOf(typeof(REPARSE_DATA_BUFFER));
            var outBuffer = Marshal.AllocHGlobal(outBufferSize);

            try
            {
                var result = DeviceIoControl(handle.DangerousGetHandle(), WIN32_FSCTL_GET_REPARSE_POINT,
                    IntPtr.Zero, 0, outBuffer, outBufferSize, out var bytesReturned, IntPtr.Zero);

                if (!result)
                {
                    var error = Marshal.GetLastWin32Error();
                    if (error == WIN32_ERROR_NOT_A_REPARSE_POINT)
                    {
                        return DirectoryReparseType.None;
                    }

                    throw GetLastWin32Error("Unable to get information about reparse point.");
                }

                var reparseDataBuffer = (REPARSE_DATA_BUFFER)Marshal.PtrToStructure(outBuffer, typeof(REPARSE_DATA_BUFFER));

                if (reparseDataBuffer.ReparseTag == WIN32_IO_REPARSE_TAG_MOUNT_POINT)
                {
                    return DirectoryReparseType.MountPoint;
                }
                if (reparseDataBuffer.ReparseTag == WIN32_IO_REPARSE_TAG_SYMLINK)
                {
                    return DirectoryReparseType.Symlink;
                }

                return DirectoryReparseType.Unknown;
            }
            finally
            {
                Marshal.FreeHGlobal(outBuffer);
            }
        }

        private static SafeFileHandle OpenDirHandle(string dirPath, FileAccessDword accessMode)
        {
            var reparsePointHandle = CreateFile(dirPath, accessMode,
                FileShareDword.Read | FileShareDword.Write | FileShareDword.Delete,
                IntPtr.Zero, CreationDispositionDword.OpenExisting,
                FileAttributesDword.BackupSemantics | FileAttributesDword.OpenReparsePoint, IntPtr.Zero);

            var err = Marshal.GetLastWin32Error();
            if (err == WIN32_ERROR_FILE_NOT_FOUND)
            {
                throw new DirectoryNotFoundException($"Cannot find the directory '{dirPath}'");
            }

            if (err != 0)
            {
                var winex = GetLastWin32Error($"Cannot open directory at path '{dirPath}'");
                throw winex.InnerException is IOException ioex ? ioex : winex;
            }

            return reparsePointHandle;
        }

        /// <summary>
        /// Wrap the native error code in an exception so we put a message.
        /// </summary>
        private static IOException GetLastWin32Error(string message)
            => new IOException(message, Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error()));
    }
}
