from glob import glob
import os
import sys
import traceback

def run_test(test_name, out_name, err_name):
  out_file = open(out_name, "w")
  err_file = open(err_name, "w")
  sys.stdout = out_file
  sys.stderr = err_file
  result = 1
  try:
    try:
      __import__(test_name)
    except KeyboardInterrupt:
      raise
    except SystemExit, e:
      sys.stderr.write("Exit code %s\n" % e)
      result = 0
    except:
      traceback.print_exc()
      result = 0
  finally:
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
  out_file.close()
  err_file.close()
  return result

def check_result(out_name, out2_name):
  return read_file(out_name) == read_file(out2_name)

def read_file(name):
  f = open(name, "rU")
  data = f.read()
  f.close()
  return data

def remove(name):
  try:
    os.unlink(name)
  except:
    pass

def run():
  if len(sys.argv) > 1:
    tests = sys.argv[1:]
  else:
    tests = glob("test?*.py")
  for test_py in tests:
    test_name = os.path.splitext(test_py)[0]
    test_out = test_name + ".out"
    test_out2 = test_name + ".out2"
    test_err = test_name + ".err"
    if os.path.exists(test_out):
      print "%s:" % test_name,
      sys.stdout.flush()
      succeeded = run_test(test_name, test_out2, test_err)
      if succeeded:
        succeeded = check_result(test_out, test_out2)
        if succeeded:
          print "passed"
        else:
          print "failed *****"
      else:
        print "error *****"
    else:
      print "creating %s:" % test_out,
      sys.stdout.flush()
      succeeded = run_test(test_name, test_out, test_err)
      if succeeded:
        print "succeeded"
      else:
        print "error *****"
    if succeeded:
      remove(test_out2)
      remove(test_err)

if __name__ == "__main__":
  run()


        

