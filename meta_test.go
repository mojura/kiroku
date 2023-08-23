package kiroku

import "testing"

func TestMeta_IsEmpty(t *testing.T) {
	type fields struct {
		LastProcessedTimestamp int64
		LastProcessedType      Type
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name:   "empty",
			fields: fields{},
			want:   true,
		},
		{
			name: "has timestamp",
			fields: fields{
				LastProcessedTimestamp: 1,
			},
			want: false,
		},
		{
			name: "empty",
			fields: fields{
				LastProcessedType: TypeChunk,
			},
			want: false,
		},
		{
			name: "empty",
			fields: fields{
				LastProcessedTimestamp: 1,
				LastProcessedType:      TypeChunk,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Meta{
				LastProcessedTimestamp: tt.fields.LastProcessedTimestamp,
				LastProcessedType:      tt.fields.LastProcessedType,
			}
			if got := m.IsEmpty(); got != tt.want {
				t.Errorf("Meta.IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}
