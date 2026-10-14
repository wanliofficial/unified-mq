#!/usr/bin/env python3
# S0.4 verification: Apache Qpid Proton (AMQP 1.0) minimal import.
# Run: pip3 install --target ./pylibs proton && PYTHONPATH=./pylibs python3 amqp_python.py
try:
    import proton
    print("proton (qpid-proton) imported OK; version:", getattr(proton, "__version__", "?"))
except Exception as e:
    print("proton import failed:", e)
