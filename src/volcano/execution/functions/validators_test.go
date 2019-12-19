package functions

import (
	"testing"

	"volcano/common"
)

func Test_exactlyNArgs(t *testing.T) {
	type args struct {
		n    int
		args []common.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "matching number",
			args: args{
				n:    2,
				args: []common.Value{common.MakeInt(7), common.MakeString("a")},
			},
			wantErr: false,
		},
		{
			name: "non-matching number - too long",
			args: args{
				n:    2,
				args: []common.Value{common.MakeInt(7), common.MakeString("a"), common.MakeBool(true)},
			},
			wantErr: true,
		},
		{
			name: "non-matching number - too short",
			args: args{
				n:    4,
				args: []common.Value{common.MakeInt(7), common.MakeString("a"), common.MakeBool(true)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ExactlyNArgs(tt.args.n).Validate(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("ExactlyNArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_atLeastNArgs(t *testing.T) {
	type args struct {
		n    int
		args []common.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "one arg - pass",
			args: args{
				1,
				[]common.Value{common.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				1,
				[]common.Value{common.MakeInt(1), common.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				1,
				[]common.Value{},
			},
			wantErr: true,
		},
		{
			name: "one arg - fail",
			args: args{
				2,
				[]common.Value{common.MakeInt(1)},
			},
			wantErr: true,
		},
		{
			name: "two args - pass",
			args: args{
				2,
				[]common.Value{common.MakeInt(1), common.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				2,
				[]common.Value{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AtLeastNArgs(tt.args.n).Validate(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("atLeastOneArg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_atMostNArgs(t *testing.T) {
	type args struct {
		n    int
		args []common.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "one arg - pass",
			args: args{
				1,
				[]common.Value{common.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - fail",
			args: args{
				1,
				[]common.Value{common.MakeInt(1), common.MakeString("hello")},
			},
			wantErr: true,
		},
		{
			name: "zero args - pass",
			args: args{
				1,
				[]common.Value{},
			},
			wantErr: false,
		},
		{
			name: "one arg - pass",
			args: args{
				2,
				[]common.Value{common.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				2,
				[]common.Value{common.MakeInt(1), common.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - pass",
			args: args{
				2,
				[]common.Value{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AtMostNArgs(tt.args.n).Validate(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("atMostOneArg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wantedType(t *testing.T) {
	type args struct {
		wantedType common.Value
		arg        common.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - int - pass",
			args: args{
				common.ZeroInt(),
				common.MakeInt(7),
			},
			wantErr: false,
		},
		{
			name: "int - float - fail",
			args: args{
				common.ZeroInt(),
				common.MakeFloat(7.0),
			},
			wantErr: true,
		},
		{
			name: "int - string - fail",
			args: args{
				common.ZeroInt(),
				common.MakeString("aaa"),
			},
			wantErr: true,
		},
		{
			name: "float - float - pass",
			args: args{
				common.ZeroFloat(),
				common.MakeFloat(7.0),
			},
			wantErr: false,
		},
		{
			name: "float - float - pass",
			args: args{
				common.ZeroFloat(),
				common.MakeFloat(7.0),
			},
			wantErr: false,
		},
		{
			name: "float - string - fail",
			args: args{
				common.ZeroFloat(),
				common.MakeString("aaa"),
			},
			wantErr: true,
		},
		{
			name: "bool - bool - pass",
			args: args{
				common.ZeroBool(),
				common.MakeBool(false),
			},
			wantErr: false,
		},
		{
			name: "string - string - pass",
			args: args{
				common.ZeroString(),
				common.MakeString("nice"),
			},
			wantErr: false,
		},
		{
			name: "string - int - fail",
			args: args{
				common.ZeroString(),
				common.MakeInt(7),
			},
			wantErr: true,
		},
		{
			name: "string - float - fail",
			args: args{
				common.ZeroString(),
				common.MakeFloat(7.0),
			},
			wantErr: true,
		},
		{
			name: "string - string - pass",
			args: args{
				common.ZeroString(),
				common.MakeString("aaa"),
			},
			wantErr: false,
		},
		{
			name: "tuple - tuple - pass",
			args: args{
				common.ZeroTuple(),
				common.MakeTuple(common.Tuple{common.MakeInt(1), common.MakeInt(2), common.MakeInt(3)}),
			},
			wantErr: false,
		},
		{
			name: "tuple - int - fail",
			args: args{
				common.ZeroTuple(),
				common.MakeInt(4),
			},
			wantErr: true,
		},
		{
			name: "object - object - pass",
			args: args{
				common.ZeroObject(),
				common.MakeObject(map[string]common.Value{}),
			},
			wantErr: false,
		},
		{
			name: "object - int - fail",
			args: args{
				common.ZeroObject(),
				common.MakeInt(4),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := TypeOf(tt.args.wantedType).Validate(tt.args.arg); (err != nil) != tt.wantErr {
				t.Errorf("basicType() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
