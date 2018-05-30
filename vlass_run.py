from management import execute
import vlass2caom2

if __name__ == "__main__":
    execute.run_by_file('VLASS',
                        vlass2caom2.VlassNameHandler,
                        vlass2caom2.VlassArgsPassThrough)
