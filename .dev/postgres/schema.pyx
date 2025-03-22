from .parameters cimport PGSchemaParameters
cimport cython


@cython.final
@cython.no_gc_clear
cdef class PGSchemaDatamodel:
    cdef:
        readonly PGSchemaParameters parameters
        
    def __cinit__(self, PGSchemaParameters parameters):
        self.parameters = parameters

    def __dealloc__(self):
        self.parameters = None

    def __richcmp__(self, PGSchemaDatamodel other, int op):
        if other is None:
            if op == 2:  # ==
                return False
            elif op == 3:  # !=
                return True
            return NotImplemented
        
        if not isinstance(other, PGSchemaDatamodel):
            return NotImplemented
            
        # Compare parameters
        if op == 2:  # ==
            return self.parameters == other.parameters
        elif op == 3:  # !=
            return self.parameters != other.parameters
        else:
            return NotImplemented