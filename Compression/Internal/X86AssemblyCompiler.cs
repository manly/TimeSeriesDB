using System;
using System.Diagnostics;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.Security;
using System.Linq;


#region TO GENERATE x86/x64 ASSEMBLY CODE USING VC++
/* 
In order to generate the assembly I started a new VC++ project, 
created the functions I wanted, then went to Debug-->Windows-->Disassembly. 
For compiler options I disabled inlining, enabled intrinsics, favored fast code, 
omitted frame pointers, disabled security checks and SDL (Security Development Lifecycle ) checks.

===============================

#include "stdafx.h"
#include <intrin.h>

#pragma intrinsic(_BitScanForward)
#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanForward64)
#pragma intrinsic(_BitScanReverse64)

__declspec(noinline) int _cdecl BitScanForward(unsigned int value) {
    unsigned long i;
    return _BitScanForward(&i, value) ? i : -1;
}
__declspec(noinline) int _cdecl BitScanForward64(unsigned long long value) {
    unsigned long i;
    return _BitScanForward64(&i, value) ? i : -1;
}
__declspec(noinline) int _cdecl BitScanReverse(unsigned int value) {
    unsigned long i;
    return _BitScanReverse(&i, value) ? i : -1;
}
__declspec(noinline) int _cdecl BitScanReverse64(unsigned long long value) {
    unsigned long i;
    return _BitScanReverse64(&i, value) ? i : -1;
}
*/
#endregion
#region TO GENERATE x86/x64 ASSEMBLY CODE USING GCC
/* 
gcc -S -o test.asm test.c

===============================

gcc -S -O2 -o test.asm test.c

// clz = count leading zeroes
unsigned int clz(unsigned int num) {
    return __builtin_clz(num);
}
// ctz = count trailing zeroes
unsigned int ctz(unsigned int num) {
    return __builtin_ctz(num);
}
*/
#endregion


namespace TimeSeriesDB.Internal
{

    /// <summary>
    ///     Compiles/Runs x86/x64 assembly code.
    ///     This is especially useful to run CPU intrinsics.
    ///     Note that this forces calling a delegate, which negates any performance boost.
    ///     It is more intended to run custom code than performance code.
    /// </summary>
    [SuppressUnmanagedCodeSecurity] // perf speedup
    public static class X86AssemblyCompiler {
        #region static BitScanForward()
        private static readonly CompiledFunction<BitScan32Delegate> m_bitScanForward32Internal = GenerateBitScanForward32();
        private static readonly CompiledFunction<BitScan64Delegate> m_bitScanForward64Internal = GenerateBitScanForward64();
        private static readonly BitScan32Delegate m_bitScanForward32 = m_bitScanForward32Internal.Compiled;
        private static readonly BitScan64Delegate m_bitScanForward64 = m_bitScanForward64Internal.Compiled;
        /// <summary>
        ///     Returns the position of the right-most non-zero bit (__builtin_ctz).
        ///     ex: '1100 0000' = 6
        ///     Returns -1 if value=0.
        ///     This uses the BSF assembly mnemonic.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int BitScanForward(uint value) => m_bitScanForward32(value);
        /// <summary>
        ///     Returns the position of the right-most non-zero bit (__builtin_ctz).
        ///     ex: '1100 0000' = 6
        ///     Returns -1 if value=0.
        ///     This uses the BSF assembly mnemonic.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int BitScanForward(ulong value) => m_bitScanForward64(value);
        #endregion
        #region static BitScanReverse()
        private static readonly CompiledFunction<BitScan32Delegate> m_bitScanReverse32Internal = GenerateBitScanReverse32();
        private static readonly CompiledFunction<BitScan64Delegate> m_bitScanReverse64Internal = GenerateBitScanReverse64();
        private static readonly BitScan32Delegate m_bitScanReverse32 = m_bitScanReverse32Internal.Compiled;
        private static readonly BitScan64Delegate m_bitScanReverse64 = m_bitScanReverse64Internal.Compiled;
        /// <summary>
        ///     Returns the position of the left-most non-zero bit (31-__builtin_clz).
        ///     ex: '0011 0000' = 5
        ///     Returns -1 if value=0.
        ///     This uses the BSR assembly mnemonic.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int BitScanReverse(uint value) => m_bitScanReverse32(value);
        /// <summary>
        ///     Returns the position of the left-most non-zero bit (63-__builtin_clz).
        ///     ex: '0011 0000' = 5
        ///     Returns -1 if value=0.
        ///     This uses the BSR assembly mnemonic.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int BitScanReverse(ulong value) => m_bitScanReverse64(value);
        #endregion

        #region static GenerateX86Function()
        /// <summary>
        ///     Builds a dynamic method in MSIL using the calli opcode for maximum speed.
        ///     This will run faster than a delegate created with Marshal.GetDelegateForFunctionPointer().
        ///     Make sure to keep a pointer to the returned type, as the compiled delegate will not work otherwise.
        /// </summary>
        /// <typeparam name="TDelegate">Make sure the delegate has [System.Security.SuppressUnmanagedCodeSecurity] for better performance, as well as [System.Runtime.InteropServices.UnmanagedFunctionPointer(System.Runtime.InteropServices.CallingConvention.Cdecl)].</typeparam>
        public static CompiledFunction<TDelegate> GenerateX86Function<TDelegate>(string methodName, byte[] x86AssemblyBytes) {
            var handle = MarkMemoryAsExecutable(x86AssemblyBytes);

            var invokeMethod = (_MethodInfo)typeof(TDelegate).GetMember("Invoke")[0];
            Type returnType = invokeMethod.ReturnType;
            Type[] parameterTypes = invokeMethod.GetParameters().Select(o => o.ParameterType).ToArray();

            var method = new DynamicMethod(methodName, returnType, parameterTypes, returnType.Module);
            var gen = method.GetILGenerator();

            var opcodes = new[] { OpCodes.Ldarg_0, OpCodes.Ldarg_1, OpCodes.Ldarg_2, OpCodes.Ldarg_3 };
            for(int i=0; i<parameterTypes.Length; i++)
                gen.Emit(opcodes[i]);
            
            gen.Emit(OpCodes.Ldc_I8, (long)handle.AddrOfPinnedObject());
            gen.Emit(OpCodes.Conv_I);
            gen.EmitCalli(OpCodes.Calli, CallingConvention.StdCall, returnType, parameterTypes);
            gen.Emit(OpCodes.Ret);
            
            return new CompiledFunction<TDelegate>(
                methodName,
                x86AssemblyBytes,
                handle,
                (TDelegate)(object)method.CreateDelegate(typeof(TDelegate)));
        }
        /// <summary>
        ///     A class to keep the pointers of everything necessary for the compiled delegate to work.
        /// </summary>
        public class CompiledFunction<TDelegate> : IDisposable {
            public readonly TDelegate Compiled;
            public readonly byte[] X86AssemblyBytes; // to keep in memory
            public readonly GCHandle Handle;
            public readonly string Name;
            #region constructors
            public CompiledFunction(string name, byte[] x86assemblyBytes, GCHandle handle, TDelegate _delegate) {
                this.Name = name;
                this.Compiled = _delegate;
                this.X86AssemblyBytes = x86assemblyBytes;
                this.Handle = handle;
            }
            #endregion
            #region IDisposable Support
            private bool disposedValue = false;
            protected virtual void Dispose(bool disposing) {
                if(!disposedValue) {
                    if(disposing)
                        this.Handle.Free();
                    disposedValue = true;
                }
            }
            public void Dispose() {
                Dispose(true);
            }
            #endregion
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool VirtualProtect(IntPtr address, IntPtr size, uint protect, out uint oldProtect);
        /// <param name="x86AssemblyBytes">Those bytes must be held in memory by a pointer.</param>
        private static GCHandle MarkMemoryAsExecutable(byte[] x86AssemblyBytes) {
            const uint PAGE_EXECUTE_READWRITE = 0x40;

            var handle = GCHandle.Alloc(x86AssemblyBytes, GCHandleType.Pinned);
            VirtualProtect(handle.AddrOfPinnedObject(), (IntPtr)x86AssemblyBytes.Length, PAGE_EXECUTE_READWRITE, out uint old);
            return handle;
        }
        #endregion
        #region static GenerateX86Function_Slow()
        /// <summary>
        ///     Compiled a slightly slower to execute delegate than its counterpart function, but in a much more safe way without dynamic MSIL generation code.
        /// </summary>
        /// <typeparam name="TDelegate">Make sure the delegate has [System.Security.SuppressUnmanagedCodeSecurity] for better performance, as well as [System.Runtime.InteropServices.UnmanagedFunctionPointer(System.Runtime.InteropServices.CallingConvention.Cdecl)].</typeparam>
        public static TDelegate GenerateX86Function_Slow<TDelegate>(byte[] x86AssemblyBytes) {
            var bufferPtr = AllocExecutableMemory(x86AssemblyBytes);
            return (TDelegate)(object)Marshal.GetDelegateForFunctionPointer(bufferPtr, typeof(TDelegate));
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr VirtualAlloc(IntPtr lpAddress, uint dwSize, uint flAllocationType, uint flProtect);
        private static IntPtr AllocExecutableMemory(byte[] x86AssemblyBytes) {
            const uint PAGE_EXECUTE_READWRITE = 0x40;
            const uint ALLOCATIONTYPE_MEM_COMMIT = 0x1000;
            const uint ALLOCATIONTYPE_RESERVE = 0x2000;
            const uint ALLOCATIONTYPE = ALLOCATIONTYPE_MEM_COMMIT | ALLOCATIONTYPE_RESERVE;

            var bufferPtr = VirtualAlloc(IntPtr.Zero, unchecked((uint)x86AssemblyBytes.Length), ALLOCATIONTYPE, PAGE_EXECUTE_READWRITE);
            Marshal.Copy(x86AssemblyBytes, 0, bufferPtr, x86AssemblyBytes.Length);

            return bufferPtr;
        }
        #endregion

        #region private static GenerateBitScanForward32()
        [SuppressUnmanagedCodeSecurity] // for better performance
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int BitScan32Delegate(uint value);

        private static CompiledFunction<BitScan32Delegate> GenerateBitScanForward32() {
            if(IntPtr.Size == 8) {
                return GenerateX86Function<BitScan32Delegate>(nameof(GenerateBitScanForward32), new byte[] {
                    0x0F, 0xBC, 0xD1,              // bsf         edx,ecx
                    0xB8, 0xFF, 0xFF, 0xFF, 0xFF,  // mov         eax,-1
                    0x0F, 0x45, 0xC2,              // cmovne      eax,edx
                    0xC3,                          // ret
                });
            } else if(IntPtr.Size == 4) {
                return GenerateX86Function<BitScan32Delegate>(nameof(GenerateBitScanForward32), new byte[] {
                    0x51,                          // push        ecx
                    0x0F, 0xBC, 0xC1,              // bsf         eax,ecx
                    0x89, 0x04, 0x24,              // mov         dword ptr [esp],eax
                    0xB8, 0xFF, 0xFF, 0xFF, 0xFF,  // mov         eax,-1
                    0x0F, 0x45, 0x04, 0x24,        // cmovne      eax,dword ptr [esp]
                    0x59,                          // pop         ecx
                    0xC3,                          // ret
                });
            }
            return null;
        }
        #endregion
        #region private static GenerateBitScanForward64()
        [SuppressUnmanagedCodeSecurity] // for better performance
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int BitScan64Delegate(ulong value);

        private static CompiledFunction<BitScan64Delegate> GenerateBitScanForward64() {
            if(IntPtr.Size == 8) {
                return GenerateX86Function<BitScan64Delegate>(nameof(GenerateBitScanForward64), new byte[] {
                    0x48, 0x0F, 0xBC, 0xD1,        // bsf         rdx,rcx
                    0xB8, 0xFF, 0xFF, 0xFF, 0xFF,  // mov         eax,-1
                    0x0F, 0x45, 0xC2,              // cmovne      eax,edx
                    0xC3,                          // ret
                });
            } else if(IntPtr.Size == 4) {
                // could recode by hand here using __builtin_ctz code
                throw new PlatformNotSupportedException();
            }
            return null;
        }
        #endregion
        #region private static GenerateBitScanReverse32()
        private static CompiledFunction<BitScan32Delegate> GenerateBitScanReverse32() {
            if(IntPtr.Size == 8) {
                return GenerateX86Function<BitScan32Delegate>(nameof(GenerateBitScanReverse32), new byte[] {
                    0x0F, 0xBD, 0xD1,              // bsr         edx,ecx
                    0xB8, 0xFF, 0xFF, 0xFF, 0xFF,  // mov         eax,-1
                    0x0F, 0x45, 0xC2,              // cmovne      eax,edx
                    0xC3,                          // ret
                });
            } else if(IntPtr.Size == 4) {
                return GenerateX86Function<BitScan32Delegate>(nameof(GenerateBitScanReverse32), new byte[] {
                    0x51,                          // push        ecx
                    0x0F, 0xBD, 0xC1,              // bsr         eax,ecx
                    0x89, 0x04, 0x24,              // mov         dword ptr [esp],eax
                    0xB8, 0xFF, 0xFF, 0xFF, 0xFF,  // mov         eax,-1
                    0x0F, 0x45, 0x04, 0x24,        // cmovne      eax,dword ptr [esp]
                    0x59,                          // pop         ecx
                    0xC3,                          // ret
                });
            }
            return null;
        }
        #endregion
        #region private static GenerateBitScanReverse64()
        private static CompiledFunction<BitScan64Delegate> GenerateBitScanReverse64() {
            if(IntPtr.Size == 8) {
                return GenerateX86Function<BitScan64Delegate>(nameof(GenerateBitScanReverse64), new byte[] {
                    0x48, 0x0F, 0xBD, 0xD1,        // bsr         rdx,rcx
                    0xB8, 0xFF, 0xFF, 0xFF, 0xFF,  // mov         eax,-1
                    0x0F, 0x45, 0xC2,              // cmovne      eax,edx
                    0xC3,                          // ret
                });
            } else if(IntPtr.Size == 4) {
                // could recode by hand here using __builtin_clz code
                throw new PlatformNotSupportedException();
            }
            return null;
        }
        #endregion
    }
}
