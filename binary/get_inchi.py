import sys
import os

import subprocess

"""python get_inchi.py example/input.sdf sdf 0 0"""

input_mol_file_path = sys.argv[1]
params = {'file_type': sys.argv[2],  # mol, sdf, pdb, cdx, smi
          'convert_3d_flag': sys.argv[3],  # 0, 1
          'add_protons_flag': sys.argv[4]  # 0, 1
          }


def apply_aux_functions(dir_path, input_file_name, params):
    output_file_name = input_file_name
    command = ['babel',
               '-i%s' % params['file_type'],
               os.path.join(dir_path, input_file_name)]

    if params['convert_3d_flag'] == '1':
        command.append('--gen3d')
    if params['add_protons_flag'] == '1':
        command.append('-h')
    if params['convert_3d_flag'] == '1' or params['add_protons_flag'] == '1' \
            or (params['file_type'] != 'mol' and params['file_type'] != 'sdf'):
        output_file_name = 'alatis_obabel_converted_%s.sdf' % output_file_name
        command.extend(['-osdf', '-O', os.path.join(dir_path, output_file_name)])
        subprocess.call(command)

    return output_file_name


dir_path = os.path.dirname(input_mol_file_path)
input_file_name = os.path.basename(input_mol_file_path)

""" check and apply auxiliary functions -- obabel required """
aux_file_name = apply_aux_functions(dir_path, input_file_name, params)

inchi = ''
if os.path.exists(os.path.join(dir_path, aux_file_name)):

    #  run inchi-1
    subprocess.call(['./inchi-1', os.path.join(dir_path, aux_file_name)])

    #  parse output
    output_complete_path = os.path.join(dir_path, aux_file_name + '.txt')
    if os.path.exists(output_complete_path):
        fin = open(output_complete_path, 'r')
        for a_line in fin:
            if 'InChI=' in a_line:
                inchi = a_line.strip()
                break
        fin.close()

    #  cleaning
    os.system('rm -f %s' % os.path.join(dir_path, aux_file_name + '.txt'))
    os.system('rm -f %s' % os.path.join(dir_path, aux_file_name + '.log'))
    os.system('rm -f %s' % os.path.join(dir_path, aux_file_name + '.prb'))

with open('inchi.txt', 'w') as inchi_file:
    inchi_file.write(inchi)
